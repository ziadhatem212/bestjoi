// npm i ws && node server.js   (serves index.html + WebSocket on the same port)
const http=require('http'),fs=require('fs'),crypto=require('crypto'),{WebSocketServer}=require('ws');
const AL='ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // no look-alike chars; 8 chars ~ 40 bits
const S=new Map(),fails=new Map();
const tok=()=>crypto.randomBytes(24).toString('hex');
const mk=()=>{let c;do{c=Array.from({length:8},()=>AL[crypto.randomInt(AL.length)]).join('')}while(S.has(c));return c};
const eq=(a,b)=>{a=Buffer.from(String(a||''));b=Buffer.from(String(b||''));return a.length===b.length&&crypto.timingSafeEqual(a,b)};
const send=(w,o)=>{if(w&&w.readyState===1)w.send(JSON.stringify(o))};
function kick(s,rot){send(s.ctrl,{t:'kicked'});if(s.ctrl)s.ctrl.close();s.ctrl=null;s.ctok=null;send(s.owner,{t:'ctrl',on:false});
 if(rot){S.delete(s.code);s.code=mk();S.set(s.code,s);send(s.owner,{t:'created',code:s.code,token:s.otok})}}

const srv=http.createServer((q,r)=>{
 if(q.url.split('?')[0]==='/'){r.writeHead(200,{'content-type':'text/html; charset=utf-8'});fs.createReadStream(__dirname+'/index.html').pipe(r)}
 else{r.writeHead(404);r.end()}});
const wss=new WebSocketServer({server:srv,maxPayload:4096});

wss.on('connection',(w,q)=>{
 const ip=q.headers['x-forwarded-for']?.split(',')[0].trim()||q.socket.remoteAddress;
 w.alive=true;w.on('pong',()=>w.alive=true);let s=null,role=null;
 w.on('message',d=>{let m;try{m=JSON.parse(d)}catch{return}
  if(m.t==='create'&&role!=='c'){if(s)S.delete(s.code);
   const c=mk();s={code:c,otok:tok(),ctok:null,owner:w,ctrl:null,allow:true,made:Date.now(),last:Date.now()};S.set(c,s);role='o';
   send(w,{t:'created',code:c,token:s.otok})}
  else if(m.t==='resume'&&role!=='c'){const x=S.get(String(m.code));
   if(x&&eq(x.otok,m.token)){s=x;role='o';s.owner=w;s.last=Date.now();send(w,{t:'created',code:s.code,token:s.otok,ctrl:!!s.ctrl});send(s.ctrl,{t:'owner',on:true})}
   else send(w,{t:'err',e:'expired'})}
  else if(m.t==='join'&&role!=='o'){
   const f=fails.get(ip)||{n:0,t:Date.now()};if(Date.now()-f.t>6e4){f.n=0;f.t=Date.now()}
   if(f.n>=8)return send(w,{t:'err',e:'rate'});
   const x=S.get(String(m.code||'').toUpperCase().replace(/[^A-Z0-9]/g,''));
   if(!x||!x.allow){f.n++;fails.set(ip,f);return send(w,{t:'err',e:'bad'})}
   const re=x.ctok&&eq(x.ctok,m.token);
   if(!re&&x.ctrl&&x.ctrl.readyState===1)return send(w,{t:'err',e:'busy'});
   if(re&&x.ctrl&&x.ctrl!==w){send(x.ctrl,{t:'kicked'});x.ctrl.close()}
   if(!re)x.ctok=tok();
   s=x;role='c';x.ctrl=w;send(w,{t:'joined',token:x.ctok});send(x.owner,{t:'ctrl',on:true});send(w,{t:'owner',on:!!x.owner})}
  else if(!s)return;
  else if(role==='o'){s.last=Date.now();
   if(m.t==='state')send(s.ctrl,m);
   else if(m.t==='allow'){s.allow=!!m.v;if(!s.allow)kick(s)}
   else if(m.t==='kick')kick(s,true)}
  else if(role==='c'&&m.t==='cmd'&&s.allow&&s.ctrl===w)send(s.owner,m)});
 w.on('close',()=>{if(!s)return;
  if(role==='o'&&s.owner===w){s.owner=null;s.last=Date.now();send(s.ctrl,{t:'owner',on:false})}
  else if(role==='c'&&s.ctrl===w){s.ctrl=null;send(s.owner,{t:'ctrl',on:false})}})});

setInterval(()=>{const n=Date.now();
 for(const[c,s]of S)if(n-s.made>6*3600e3||(!s.owner&&n-s.last>30*60e3)){kick(s);if(s.owner)s.owner.close();S.delete(c)}
 for(const[k,f]of fails)if(n-f.t>6e4)fails.delete(k);
 wss.clients.forEach(w=>{if(!w.alive)return w.terminate();w.alive=false;w.ping()})},30000);
srv.listen(process.env.PORT||3000,()=>console.log('Running on port',process.env.PORT||3000));
