(async function () {
  // ==== VIDEO (как было) ====
  const video = document.getElementById('video');
  const canvas = document.getElementById('canvas');
  const ctx = canvas.getContext('2d');
  const status = document.getElementById('status');
  const emotion = document.getElementById('emotion');
  const probs = document.getElementById('probs');
  const startBtn = document.getElementById('startBtn');
  const stopBtn = document.getElementById('stopBtn');
  let ws, timer;

  async function initCam() {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ video: true });
      video.srcObject = stream;
      await video.play();
      status.textContent = 'Камера готова';
    } catch (e) {
      status.textContent = 'Ошибка камеры: ' + e.message;
    }
  }

  function connect() {
    ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws');
    ws.onmessage = e => {
      const d = JSON.parse(e.data);
      if (d.type === 'prediction') {
        emotion.textContent = d.top_emotion + ' ' + (d.score ? (d.score * 100).toFixed(1) + '%' : '');
        probs.innerHTML = Object.entries(d.probabilities || {})
          .map(([k, v]) => `<span class='tag'>${k}: ${(v * 100).toFixed(0)}%</span>`)
          .join('');
      }
    };
  }

  function start() {
    connect();
    timer = setInterval(() => {
      canvas.width = video.videoWidth / 2;
      canvas.height = video.videoHeight / 2;
      ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
      canvas.toBlob(b => {
        const r = new FileReader();
        r.onloadend = () => ws.send(JSON.stringify({ type: 'frame', data_url: r.result }));
        r.readAsDataURL(b);
      }, 'image/jpeg', 0.8);
    }, 200);
    startBtn.disabled = true; stopBtn.disabled = false;
  }
  function stop() { clearInterval(timer); startBtn.disabled = false; stopBtn.disabled = true; }

  startBtn.onclick = start; stopBtn.onclick = stop;
  initCam();

  // ==== SPEECH ====
  const speechPartial = document.getElementById('speech-partial');
  const speechFinal = document.getElementById('speech-final');
  const micStart = document.getElementById('micStart');
  const micStop = document.getElementById('micStop');
  const micSelect = document.getElementById('micSelect'); // <select id="micSelect"></select> в HTML

  let wsSpeech, audioCtx, workletNode, silentGain, mediaStream, spNode;
  let selectedDeviceId = null;
  let awHadSignal = false;
  let fallbackTimer;

  let lastFinalText = "";      // последняя финальная распознавалка, уже показанная как "→ ..."
  let awaitingAnswer = false;  // ждём ли ответ для lastFinalText
  function norm(s) { return (s || "").trim().replace(/\s+/g, " "); }

  // утилита для добавления строк в итоговый блок
  function appendDialog(lines) {
    const text = Array.isArray(lines) ? lines.join('\n') : String(lines || '');
    if (!text) return;
    speechFinal.textContent += (speechFinal.textContent ? '\n' : '') + text;
  }

  // Список микрофонов
  async function listMicrophones() {
    try {
      const devices = await navigator.mediaDevices.enumerateDevices();
      if (!micSelect) return;
      const inputs = devices.filter(d => d.kind === 'audioinput');
      micSelect.innerHTML = '';
      inputs.forEach((d, i) => {
        const opt = document.createElement('option');
        opt.value = d.deviceId;
        opt.textContent = d.label || `Микрофон ${i + 1}`;
        micSelect.appendChild(opt);
      });
      if (inputs.length > 0) {
        selectedDeviceId = micSelect.value || inputs[0].deviceId;
      }
      micSelect.onchange = () => {
        selectedDeviceId = micSelect.value;
        console.debug('🔊 выбран микрофон:', selectedDeviceId);
      };
    } catch (e) {
      console.warn('enumerateDevices failed:', e);
    }
  }

  try { await navigator.mediaDevices.getUserMedia({ audio: true }); } catch {}
  await listMicrophones();

  function openSpeechWS() {
    wsSpeech = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws_speech');
    wsSpeech.binaryType = 'arraybuffer';
    wsSpeech.onmessage = e => {
      let d; try { d = JSON.parse(e.data); } catch { return; }

      if (d.type === 'speech') {
        if (d.is_final) {
          if (d.text) {
            const t = norm(d.text);
            appendDialog(`→ ${t}`);
            lastFinalText = t;         // 👈 запоминаем, что уже показали
            awaitingAnswer = true;     // 👈 теперь ждём ответ
          }
          speechPartial.textContent = '';
        } else {
          speechPartial.textContent = d.text || '';
        }
        return;
      }

      if (d.type === 'answer') {
        const req = norm(d.request || d.text || '');
        const ans = d.answer || '';

        // 👇 не дублируем вопрос, если это тот же финальный текст, который мы уже показали
        if (!(awaitingAnswer && lastFinalText && req && req === lastFinalText)) {
          if (req) appendDialog(`→ ${req}`);
        }
        if (ans) appendDialog(`← ${ans}`);

        // сброс ожидания; следующий финал начнёт новый цикл
        awaitingAnswer = false;
        lastFinalText = "";
        return;
      }

      if (d.type === 'speech_with_answer') {
        const req = norm(d.request || d.text || '');
        const ans = d.answer || '';
        const lines = [];

        // 👇 аналогично: не дублируем, если это тот же показанный финал
        if (!(awaitingAnswer && lastFinalText && req && req === lastFinalText)) {
          if (req) lines.push(`→ ${req}`);
        }
        if (ans) lines.push(`← ${ans}`);

        appendDialog(lines);
        awaitingAnswer = false;
        lastFinalText = "";
        return;
      }

      if (d.type === 'speech_debug') {
        console.debug('speech_debug', d);
        return;
      }
    };
  }


  async function startMic() {
    micStart.disabled = true; micStop.disabled = false;

    openSpeechWS();

    mediaStream = await navigator.mediaDevices.getUserMedia({
      audio: {
        deviceId: selectedDeviceId ? { exact: selectedDeviceId } : undefined,
        channelCount: 1,
        echoCancellation: false,
        noiseSuppression: false,
        autoGainControl: false
      }
    });

    audioCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 48000 });
    await audioCtx.audioWorklet.addModule('/static/pcm-worklet.js?v=5');
    await audioCtx.resume();

    const source = audioCtx.createMediaStreamSource(mediaStream);

    workletNode = new AudioWorkletNode(audioCtx, 'pcm-downsampler', {
      numberOfInputs: 1,
      numberOfOutputs: 1,
      outputChannelCount: [1]
    });
    workletNode.channelCount = 1;
    workletNode.channelCountMode = 'explicit';
    workletNode.channelInterpretation = 'speakers';

    silentGain = audioCtx.createGain(); silentGain.gain.value = 0;

    source.connect(workletNode, 0, 0);
    workletNode.connect(silentGain);
    silentGain.connect(audioCtx.destination);

    workletNode.port.onmessage = (ev) => {
      const d = ev.data;
      if (d && d.kind === 'dbg') {
        if (d.rIn > 0.000001) awHadSignal = true;
        console.debug(`AW dbg: rIn=${d.rIn.toFixed(6)} rOut=${d.rOut.toFixed(6)} lenOut=${d.lenOut}`);
        return;
      }
      const buf = d; // ArrayBuffer PCM
      if (wsSpeech && wsSpeech.readyState === WebSocket.OPEN) {
        wsSpeech.send(new Uint8Array(buf));
      }
    };

    clearTimeout(fallbackTimer);
    awHadSignal = false;
    fallbackTimer = setTimeout(() => {
      if (!awHadSignal) {
        console.warn('AudioWorklet silent — switching to ScriptProcessor fallback');
        try { workletNode.disconnect(); silentGain.disconnect(); } catch {}
        startScriptProcessorFallback(mediaStream);
      }
    }, 1000);

    const track = mediaStream.getAudioTracks()[0];
    console.debug('gUM settings:', track && track.getSettings());
    track && (track.enabled = true);
  }

  function startScriptProcessorFallback(stream) {
    const ctx = audioCtx;
    const source = ctx.createMediaStreamSource(stream);
    spNode = ctx.createScriptProcessor(4096, 1, 1); // ~85 мс при 48k
    source.connect(spNode);
    const gainNode = ctx.createGain(); gainNode.gain.value = 0;
    spNode.connect(gainNode); gainNode.connect(ctx.destination);

    const outRate = 16000;
    let residual = new Float32Array(0);
    const gain = 2.0;

    spNode.onaudioprocess = (e) => {
      const input = e.inputBuffer.getChannelData(0); // Float32 48k
      const merged = new Float32Array(residual.length + input.length);
      merged.set(residual, 0); merged.set(input, residual.length);

      const frameIn = Math.floor(0.10 * ctx.sampleRate); // 100 мс
      let off = 0;

      while (off + frameIn <= merged.length) {
        const frame = merged.subarray(off, off + frameIn);

        const ratio = ctx.sampleRate / outRate;
        const newLen = Math.floor(frame.length / ratio);
        const ds = new Float32Array(newLen);
        let or = 0, ob = 0;
        while (or < newLen) {
          const nob = Math.round((or + 1) * ratio);
          let acc = 0, cnt = 0;
          for (let i = ob; i < nob && i < frame.length; i++) { acc += frame[i]; cnt++; }
          ds[or++] = (acc / (cnt || 1)) * gain;
          ob = nob;
        }

        const ab = new ArrayBuffer(ds.length * 2);
        const view = new DataView(ab);
        for (let i = 0; i < ds.length; i++) {
          let s = Math.max(-1, Math.min(1, ds[i]));
          view.setInt16(i * 2, s < 0 ? s * 0x8000 : s * 0x7FFF, true);
        }
        if (wsSpeech && wsSpeech.readyState === WebSocket.OPEN) {
          wsSpeech.send(new Uint8Array(ab));
        }
        off += frameIn;
      }
      residual = merged.subarray(off);
    };
  }

  async function stopMic() {
    micStop.disabled = true; micStart.disabled = false;
    try { wsSpeech && wsSpeech.close(); } catch {}
    wsSpeech = null;

    try { workletNode && workletNode.disconnect(); } catch {}
    try { silentGain && silentGain.disconnect(); } catch {}
    try { spNode && spNode.disconnect(); } catch {}
    spNode = null;

    if (mediaStream) { mediaStream.getTracks().forEach(t => t.stop()); mediaStream = null; }
    if (audioCtx) { try { await audioCtx.close(); } catch {} audioCtx = null; }
    clearTimeout(fallbackTimer);
  }

  micStart && (micStart.onclick = startMic);
  micStop && (micStop.onclick = stopMic);
})();

