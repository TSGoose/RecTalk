class PcmDownsampler extends AudioWorkletProcessor {
  constructor() {
    super();
    this._residual = new Float32Array(0);
    this._inRate = sampleRate;   // обычно 48000
    this._outRate = 16000;
    this._lastDbg = 0;
    this._dbg = { rIn: 0, rOut: 0, n: 0 };
    this._gain = 2.0; // лёгкий подъём уровня (0 = без усиления)
  }

  rms(f32) {
    if (!f32 || f32.length === 0) return 0;
    let s = 0;
    for (let i = 0; i < f32.length; i++) { const v = f32[i]; s += v*v; }
    return Math.sqrt(s / f32.length);
  }

  downsampleBuffer(input, inRate, outRate) {
    const ratio = inRate / outRate;
    const newLen = Math.floor(input.length / ratio);
    const output = new Float32Array(newLen);
    let offsetResult = 0, offsetBuffer = 0;
    while (offsetResult < newLen) {
      const nextOffsetBuffer = Math.round((offsetResult + 1) * ratio);
      let accum = 0, count = 0;
      for (let i = offsetBuffer; i < nextOffsetBuffer && i < input.length; i++) { accum += input[i]; count++; }
      output[offsetResult++] = (accum / (count || 1)) * this._gain;
      offsetBuffer = nextOffsetBuffer;
    }
    return output;
  }

  floatTo16LEPCM(float32) {
    const ab = new ArrayBuffer(float32.length * 2);
    const view = new DataView(ab);
    for (let i = 0; i < float32.length; i++) {
      let s = Math.max(-1, Math.min(1, float32[i]));
      view.setInt16(i * 2, s < 0 ? s * 0x8000 : s * 0x7FFF, true);
    }
    return ab;
  }

  process(inputs) {
    const input = inputs[0];
    if (!input || input.length === 0) return true;
    const ch0 = input[0];                  // Float32Array входного сэмпла 48k
    // Диагностика входа
    const rIn = this.rms(ch0);

    // Склейка с хвостом
    const merged = new Float32Array(this._residual.length + ch0.length);
    merged.set(this._residual, 0);
    merged.set(ch0, this._residual.length);

    const frameSizeIn = Math.floor(0.10 * this._inRate); // 100 мс
    let offset = 0;

    while (offset + frameSizeIn <= merged.length) {
      const frame = merged.subarray(offset, offset + frameSizeIn);
      const ds = this.downsampleBuffer(frame, this._inRate, this._outRate);
      const rOut = this.rms(ds);
      const pcm = this.floatTo16LEPCM(ds);
      // Отправляем PCM (transfer list, без копирования)
      this.port.postMessage(pcm, [pcm]);

      // собираем статистику для дебага
      this._dbg.rIn += rIn; this._dbg.rOut += rOut; this._dbg.n += 1;

      // раз в ~500 мс
      const t = currentTime; // аудио-время worklet
      if (t - this._lastDbg > 0.5) {
        const n = this._dbg.n || 1;
        this.port.postMessage({ kind: 'dbg', rIn: this._dbg.rIn / n, rOut: this._dbg.rOut / n, lenOut: ds.length });
        this._dbg = { rIn: 0, rOut: 0, n: 0 };
        this._lastDbg = t;
      }

      offset += frameSizeIn;
    }

    this._residual = merged.subarray(offset);
    return true;
  }
}

registerProcessor('pcm-downsampler', PcmDownsampler);

