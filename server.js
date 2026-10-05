const express = require("express");
const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const app = express();
const server = http.createServer(app);
const PORT = Number(process.env.PORT || 10000);
const MAX_PLAYERS = 32;
const MAX_ROOMS = 100;
const MAX_NAME = 24;
const MAX_CHAT = 300;
const TICK = 50;
const ROUND_TIME = 120000;

const rooms = new Map();
const clients = new Map();

function id(prefix) { return `${prefix}_${crypto.randomBytes(6).toString("hex")}`; }
function num(v, d=0) { const n=Number(v); return Number.isFinite(n)?n:d; }
function clamp(v,a,b){return Math.max(a,Math.min(b,v));}
function name(v){return String(v||"Player").replace(/[^\p{L}\p{N}_ .-]/gu,"").trim().slice(0,MAX_NAME)||"Player";}
function roomName(v){return String(v||"Nueva partida").replace(/[\u0000-\u001f\u007f]/g,"").trim().slice(0,50)||"Nueva partida";}
function send(ws,d){if(!ws||ws.readyState!==1)return false;try{ws.send(JSON.stringify(d));return true}catch{return false}}
function broadcast(room,d,except=null){for(const p of room.players.values())if(p.id!==except)send(p.ws,d)}
function roomInfo(r){return {id:r.id,name:r.name,players:r.players.size,maxPlayers:r.maxPlayers,teamSize:r.teamSize,round:r.round,maxRounds:r.maxRounds,timeLeft:Math.max(0,r.roundEndsAt-Date.now()),roundActive:r.roundActive,score:r.score}}
function pub(p){return {id:p.id,name:p.name,team:p.team,...p.state,kills:p.stats.kills,deaths:p.stats.deaths,assists:p.stats.assists}}
function snapshot(r){return {type:"room_snapshot",room:roomInfo(r),players:[...r.players.values()].map(pub)}}
function roomsList(){return [...rooms.values()].map(roomInfo).sort((a,b)=>a.name.localeCompare(b.name))}
function sendRooms(ws){send(ws,{type:"rooms",rooms:roomsList()})}
function allRooms(){for(const p of clients.values())sendRooms(p.ws)}
function err(ws,m){send(ws,{type:"error",message:String(m).slice(0,300)})}

function newState(){return {x:0,y:0,z:0,vx:0,vy:0,vz:0,yaw:0,pitch:0,onGround:true,alive:true,crouching:false,walking:false,health:100,armor:0,weapon:null,ammo:0,reserve:0}}
function assignTeam(room){let a=0,b=0;for(const p of room.players.values())p.team===0?a++:b++;return a<=b?0:1}

function publicMatch(room){return {type:"match_state",room:roomInfo(room),players:[...room.players.values()].map(pub)}}
function sendMatch(room){broadcast(room,publicMatch(room));}
function startRound(room){
  room.roundActive=true; room.roundStartedAt=Date.now(); room.roundEndsAt=Date.now()+ROUND_TIME;
  for(const p of room.players.values()){p.state.alive=true;p.state.health=100;p.state.armor=0;p.stats.roundKills=0}
  broadcast(room,{type:"round_start",round:room.round,maxRounds:room.maxRounds,timeLeft:ROUND_TIME,score:room.score});
  sendMatch(room);
}
function endRound(room){
  if(!room.roundActive)return;
  room.roundActive=false;
  broadcast(room,{type:"round_end",round:room.round,score:room.score});
  setTimeout(()=>{if(!rooms.has(room.id)||room.players.size===0)return;room.round++;if(room.round>room.maxRounds){room.round=1;room.score={0:0,1:0};}startRound(room)},2500);
}
function remove(p,announce=true){
  if(!p.roomId)return;
  const r=rooms.get(p.roomId), rid=p.roomId;p.roomId=null;
  if(!r)return;
  r.players.delete(p.id);
  if(announce)broadcast(r,{type:"player_left",id:p.id},p.id);
  send(p.ws,{type:"left_room",roomId:rid});
  if(r.players.size===0){rooms.delete(r.id)}else{sendMatch(r)}
  allRooms();
}
function join(p,r){
  if(r.players.size>=r.maxPlayers){err(p.ws,"La partida está llena.");return}
  if(p.roomId)remove(p,true);
  p.roomId=r.id;p.team=assignTeam(r);r.players.set(p.id,p);
  send(p.ws,{type:"room_joined",roomId:r.id,room:roomInfo(r),players:[...r.players.values()].map(pub)});
  broadcast(r,{type:"player_joined",player:pub(p)},p.id);
  sendMatch(r);
  if(!r.roundActive)startRound(r);
  allRooms();
}
function create(p,d){
  if(rooms.size>=MAX_ROOMS){err(p.ws,"Máximo de partidas alcanzado.");return}
  const r={id:id("room"),name:roomName(d.name),maxPlayers:clamp(num(d.maxPlayers,16),2,MAX_PLAYERS),teamSize:clamp(num(d.teamSize,8),1,16),players:new Map(),createdAt:Date.now(),round:1,maxRounds:16,roundActive:false,roundStartedAt:0,roundEndsAt:0,score:{0:0,1:0}};
  rooms.set(r.id,r);join(p,r);
}
function state(p,s){s=s&&typeof s==="object"?s:{};const q=p.state;q.x=clamp(num(s.x),-100000,100000);q.y=clamp(num(s.y),-100000,100000);q.z=clamp(num(s.z),-100000,100000);q.vx=clamp(num(s.vx),-1000,1000);q.vy=clamp(num(s.vy),-1000,1000);q.vz=clamp(num(s.vz),-1000,1000);q.yaw=num(s.yaw);q.pitch=num(s.pitch);q.onGround=!!s.onGround;q.alive=s.alive!==false;q.crouching=!!s.crouching;q.walking=!!s.walking;q.health=clamp(num(s.health,100),0,100);q.armor=clamp(num(s.armor),0,100);q.weapon=String(s.weapon||"").slice(0,64);q.ammo=clamp(num(s.ammo),0,999);q.reserve=clamp(num(s.reserve),0,999)}
function handle(p,d){
  if(!d||typeof d!=="object")return;
  switch(d.type){
    case "hello": p.name=name(d.name);send(p.ws,{type:"hello",id:p.id,playerId:p.id,version:"5.0.0"});send(p.ws,{type:"connected",id:p.id,playerId:p.id});sendRooms(p.ws);break;
    case "list_rooms":sendRooms(p.ws);break;
    case "create_room":create(p,d);break;
    case "join_room":{const r=rooms.get(String(d.roomId||""));if(!r)return err(p.ws,"La partida no existe.");join(p,r);break}
    case "leave_room":remove(p,true);break;
    case "set_name":p.name=name(d.name);if(p.roomId){const r=rooms.get(p.roomId);if(r)broadcast(r,{type:"player_state",player:pub(p)})}break;
    case "player_state":if(p.roomId){const r=rooms.get(p.roomId);if(r){state(p,d.state||d);broadcast(r,{type:"player_state",player:pub(p)},p.id)}}break;
    case "shot":if(p.roomId){const r=rooms.get(p.roomId);if(r)broadcast(r,{type:"shot",playerId:p.id,shot:d.shot||{}},p.id)}break;
    case "damage":if(p.roomId){const r=rooms.get(p.roomId);if(r){const target=r.players.get(String(d.targetId));if(!target)return;const amount=clamp(num(d.amount),0,100);target.state.health=Math.max(0,target.state.health-amount);if(target.state.health<=0)target.state.alive=false;broadcast(r,{type:"damage",attackerId:p.id,targetId:target.id,amount,headshot:!!d.headshot,weapon:String(d.weapon||"").slice(0,64),health:target.state.health},p.id)}}break;
    case "kill":if(p.roomId){const r=rooms.get(p.roomId);if(r){const t=r.players.get(String(d.targetId));if(!t)return;t.state.alive=false;t.state.health=0;t.stats.deaths++;p.stats.kills++;if(t.team!==p.team)r.score[p.team]++;broadcast(r,{type:"kill",attackerId:p.id,targetId:t.id,weapon:String(d.weapon||"").slice(0,64),headshot:!!d.headshot},p.id);sendMatch(r);if([...r.players.values()].filter(x=>x.team===0&&x.state.alive).length===0||[...r.players.values()].filter(x=>x.team===1&&x.state.alive).length===0)endRound(r)}}break;
    case "respawn":if(p.roomId){const r=rooms.get(p.roomId);if(r){p.state.alive=true;p.state.health=100;p.state.armor=0;broadcast(r,{type:"respawn",player:pub(p)},null)}}break;
    case "chat":if(p.roomId){const r=rooms.get(p.roomId);if(r){const m=String(d.message||"").replace(/[\u0000-\u001f\u007f]/g,"").trim().slice(0,MAX_CHAT);if(m)broadcast(r,{type:"chat",id:p.id,name:p.name,message:m})}}break;
    case "ping":send(p.ws,{type:"pong",t:d.t??Date.now()});break;
  }
}

app.get("/",(_,res)=>res.json({ok:true,service:"clutcher-io-online",websocket:true,rooms:rooms.size,clients:clients.size}));
app.get("/health",(_,res)=>res.json({ok:true,rooms:rooms.size,clients:clients.size}));
const wss=new WebSocketServer({server,maxPayload:64*1024});
wss.on("connection",ws=>{
  const p={id:id("player"),name:"Player",ws,roomId:null,team:0,state:newState(),stats:{kills:0,deaths:0,assists:0},connectedAt:Date.now()};clients.set(p.id,p);send(ws,{type:"connected",id:p.id,playerId:p.id});
  ws.on("message",raw=>{try{handle(p,JSON.parse(raw.toString()))}catch(e){err(ws,"JSON inválido")}});
  ws.on("close",()=>{remove(p,true);clients.delete(p.id);allRooms()});ws.on("error",()=>{});
});
setInterval(()=>{for(const r of rooms.values()){if(r.players.size===0){rooms.delete(r.id);continue}if(r.roundActive&&Date.now()>=r.roundEndsAt)endRound(r);if(r.roundActive)broadcast(r,{type:"clock",timeLeft:Math.max(0,r.roundEndsAt-Date.now())})}},TICK);
server.listen(PORT,"0.0.0.0",()=>console.log(`Clutcher Online server listening on ${PORT}`));
