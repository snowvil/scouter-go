#!/usr/bin/env python3
"""Explicit isolated replay DB verification; all artifacts live in the supplied output root."""
import importlib.util,json,os,subprocess,sys,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
out=Path(sys.argv[1]);source=Path(os.environ['SCOUTER_SOURCE'])
commands=[]
def run(args,label):
 args=list(map(str,args));p=subprocess.run(args,cwd=ROOT,stdout=(out/(label+'.log')).open('w'),stderr=subprocess.STDOUT);commands.append(dict(command=args,exit=p.returncode));(out/'commands.json').write_text(json.dumps(commands,indent=2));print(label,p.returncode,flush=True);return p.returncode
for mode in ['plain','gzip']:
 args=['dist/scouter-replay','--server','dist/scouter-go','--work-dir',out/mode,'--report',out/(mode+'.json'),'--keep-db','--duration','15s','--warmup','2s','--rate','300','--objects','10','--query-concurrency','1','--seed','7']
 if mode=='gzip':args+=['--compress-xlog','--compress-profile']
 run(args,'replay-'+mode)
 db=out/mode/'database'
 if not db.exists():
  choices=list((out/mode).rglob('obj.data'));db=choices[0].parents[2] if choices else db
 print('db',db,flush=True)
 run(['dist/scouter-db','inspect','--db',db,'--report',out/(mode+'-inspect.json')],'inspect-'+mode)
 shutil.copytree(db,out/(mode+'-java'))
spec=importlib.util.spec_from_file_location('storage_oracle',ROOT/'scripts/java-storage.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
try:
 cp=m.compile_storage(source,out/'compiled');(out/'classpath.txt').write_text(cp);commands.append(dict(command='compile_storage(source, out/compiled) using scripts/java-storage.py',exit=0))
except Exception as e:
 (out/'compile-error.log').write_text(str(e));commands.append(dict(command='compile_storage',exit=1));(out/'commands.json').write_text(json.dumps(commands,indent=2));raise
if run(['javac','--release','8','-cp',cp,'-d',out/'compiled/classes',ROOT/'tools/java-storage/ReplayDbOracle.java'],'compile-replay'):sys.exit(1)
for mode in ['plain','gzip']:run(['java','-Duser.timezone=Asia/Seoul','-cp',cp,'ReplayDbOracle',out/(mode+'-java')],'java-'+mode)
shutil.copytree(out/'gzip-java',out/'gzip-append')
dates=sorted(p.name for p in (out/'gzip-append').iterdir() if p.is_dir() and p.name.isdigit() and p.name!='00000000')
run(['java','-Duser.timezone=Asia/Seoul','-cp',cp,'ReplayDbOracle',out/'gzip-append',dates[0]],'java-append')
run(['dist/scouter-db','inspect','--db',out/'gzip-append','--report',out/'append-inspect.json'],'inspect-append')
run(['java','-Duser.timezone=Asia/Seoul','-cp',cp,'ReplayDbOracle',out/'gzip-append'],'java-after-append')
