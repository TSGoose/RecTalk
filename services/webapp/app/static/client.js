(async function(){
  const video = document.getElementById('video');
  const canvas = document.getElementById('canvas');
  const ctx = canvas.getContext('2d');
  const status = document.getElementById('status');
  const emotion = document.getElementById('emotion');
  const probs = document.getElementById('probs');
  const startBtn = document.getElementById('startBtn');
  const stopBtn = document.getElementById('stopBtn');
  let ws, timer;

  async function initCam(){
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ video:true });
      video.srcObject = stream;
      await video.play();
      status.textContent = 'Камера готова';
    } catch(e) {
      status.textContent = 'Ошибка камеры: '+e.message;
    }
  }

  function connect(){
    ws = new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');
    ws.onmessage = e=>{
      const d = JSON.parse(e.data);
      if(d.type==='prediction'){
        emotion.textContent = d.top_emotion+' '+(d.score?(d.score*100).toFixed(1)+'%':'');
        probs.innerHTML = Object.entries(d.probabilities||{}).map(([k,v])=>`<span class='tag'>${k}: ${(v*100).toFixed(0)}%</span>`).join('');
      }
    };
  }

  function start(){
    connect();
    timer=setInterval(()=>{
      canvas.width=video.videoWidth/2;canvas.height=video.videoHeight/2;
      ctx.drawImage(video,0,0,canvas.width,canvas.height);
      canvas.toBlob(b=>{
        const r=new FileReader();
        r.onloadend=()=>ws.send(JSON.stringify({type:'frame',data_url:r.result}));
        r.readAsDataURL(b);
      },'image/jpeg',0.8);
    },200);
    startBtn.disabled=true; stopBtn.disabled=false;
  }
  function stop(){ clearInterval(timer); startBtn.disabled=false; stopBtn.disabled=true; }

  startBtn.onclick=start; stopBtn.onclick=stop;
  initCam();
})();

