#!/usr/bin/env python3
"""복사한 Java Scouter의 loopback 실행/LOGIN/종료 관리 (Python 표준 라이브러리)."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

DEFAULT_SOURCE = os.environ.get('SCOUTER_SERVER_DIST')  # 원본 배포본의 server 디렉터리
DEFAULT_JDK = '/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home'
LOGIN_JAVA = r'''
import java.io.*;
import java.net.*;
import java.security.*;
import scouter.io.*;
import scouter.lang.pack.*;
public class BenchLogin {
  public static void main(String[] args) throws Exception {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress("127.0.0.1", Integer.parseInt(args[0])), 500);
      s.setSoTimeout(1000);
      DataOutputX out = new DataOutputX(new DataOutputStream(s.getOutputStream()));
      DataInputX in = new DataInputX(new DataInputStream(s.getInputStream()));
      StringBuilder pass = new StringBuilder();
      for (byte b : MessageDigest.getInstance("SHA-256").digest(
          "qwertyuiop!@#$%^&*()zxcvbnm,.admin".getBytes("UTF-8")))
        pass.append(String.format("%02x", b & 255));
      MapPack p = new MapPack(); p.put("id", "admin"); p.put("pass", pass.toString());
      out.writeInt(0xcafe2001); out.writeText("LOGIN"); out.writeLong(0);
      out.writePack(p); out.flush();
      if (in.readByte() != 3) throw new IOException("LOGIN response missing");
      MapPack response = (MapPack) in.readPack();
      if (response.getLong("session") == 0 || in.readByte() != 4)
        throw new IOException("LOGIN rejected");
      out.writeText("CLOSE"); out.flush();
      System.out.println("LOGIN_OK");
    }
  }
}
'''


def save(path, data):
    staging = path.with_suffix(path.suffix + '.tmp')
    staging.write_text(json.dumps(data, indent=2) + '\n')
    staging.replace(path)


def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def owned(data):
    if not alive(data['pid']):
        return False
    command = subprocess.check_output(['ps', '-p', str(data['pid']), '-o', 'command='], text=True)
    marker = '-Dbench.java.workdir=' + data['work_dir']
    if marker not in command.split():
        raise RuntimeError('PID 소유권 불일치: signal/삭제 거절')
    return True


def port(value):
    n = int(value)
    if not 1024 <= n <= 65535 or n in (6100, 6180):
        raise argparse.ArgumentTypeError('1024..65535 전용 포트 필요 (Scouter 기본 포트 6100/6180 금지)')
    return n


def run(args):
    ready = Path(args.ready_file).resolve()
    # stale ready 파일을 덮어써 다른 실행의 PID를 잃지 않는다.
    with ready.open('x') as f:
        f.write('{}\n')
    work = Path(tempfile.mkdtemp(prefix='scouter-java-bench-')).resolve()
    data = {'work_dir': str(work), 'state': 'starting', 'pid': 0}
    proc = None
    stopping = False

    def request_stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        source = Path(args.source).resolve()
        # 실행 파일과 동봉 lib만 복사: 원본 DB/계정/설정/스크립트/로그는 이관하지 않는다.
        shutil.copy2(source / 'scouter-server-boot.jar', work)
        shutil.copytree(source / 'lib', work / 'lib')
        removed = []
        for jar in (work / 'lib').glob('*pulse*.jar'):
            removed.append(jar.name)
            jar.unlink()
        for name in ('conf', 'plugin', 'database', 'logs'):
            (work / name).mkdir()
        settings = {
            'net_udp_listen_ip': '127.0.0.1', 'net_tcp_listen_ip': '127.0.0.1',
            'net_udp_listen_port': args.udp_port, 'net_tcp_listen_port': args.tcp_port,
            'net_http_server_enabled': 'false', 'ext_plugin_pulse_enable': 'false',
            'mgr_purge_enabled': 'false', 'db_dir': str(work / 'database'),
            'log_dir': str(work / 'logs'), 'compress_xlog_enabled': str(args.compress_xlog).lower(),
            'compress_profile_enabled': str(args.compress_profile).lower(),
            'net_udp_packet_buffer_size': 65535, '_net_udp_worker_thread_count': 3,
            'net_udp_so_rcvbuf_size': args.udp_rcvbuf,
            'xlog_queue_size': 10000, 'profile_queue_size': 1000,
        }
        conf = work / 'conf' / 'scouter.conf'
        conf.write_text(''.join(f'{k}={v}\n' for k, v in settings.items()))
        (work / 'BenchLogin.java').write_text(LOGIN_JAVA)
        java = str(Path(args.java_home) / 'bin' / 'java')
        javac = str(Path(args.java_home) / 'bin' / 'javac')
        env = {k: v for k, v in os.environ.items() if k not in
               ('JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS', 'CLASSPATH')}
        subprocess.run([javac, '-cp', str(work / 'lib' / '*'), str(work / 'BenchLogin.java')],
                       check=True, env=env, timeout=30)
        # 시작 전에 점유를 확인한다. 실제 bind/LOGIN/listener 검증도 아래에서 수행한다.
        for kind, number in ((socket.SOCK_STREAM, args.tcp_port), (socket.SOCK_DGRAM, args.udp_port)):
            with socket.socket(socket.AF_INET, kind) as sock:
                sock.bind(('127.0.0.1', number))
        jvm = args.jvm_arg or ['-Xmx512m']
        command = [java, *jvm, '-Duser.timezone=Asia/Seoul', '-Dscouter.config=' + str(conf),
                   '-Dbench.java.workdir=' + str(work), '-cp', 'scouter-server-boot.jar',
                   'scouter.boot.Boot', './lib']
        data.update({'command': command, 'settings': settings, 'removed_plugins': removed,
                     'udp': f'127.0.0.1:{args.udp_port}', 'tcp': f'127.0.0.1:{args.tcp_port}',
                     'java_version': subprocess.check_output([java, '-version'], stderr=subprocess.STDOUT, env=env, text=True),
                     'jar_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                    for p in sorted(work.rglob('*.jar'))}})
        with (work / 'server.log').open('wb') as log:
            proc = subprocess.Popen(command, cwd=work, stdout=log, stderr=subprocess.STDOUT,
                                    stdin=subprocess.DEVNULL, env=env, start_new_session=True)
        data['pid'] = proc.pid
        save(ready, data)
        deadline = time.monotonic() + args.start_timeout
        last_error = ''
        while time.monotonic() < deadline and not stopping:
            if proc.poll() is not None:
                raise RuntimeError('Java가 준비 전에 종료됨: ' + str(work / 'server.log'))
            probe = subprocess.run([java, '-Xmx64m', '-cp', str(work) + os.pathsep + str(work / 'lib' / '*'),
                                    'BenchLogin', str(args.tcp_port)], capture_output=True, text=True,
                                   timeout=3, env=env)
            if probe.returncode == 0:
                listeners = subprocess.check_output(['lsof', '-nP', '-a', '-p', str(proc.pid), '-i'], text=True)
                if (f'127.0.0.1:{args.tcp_port} (LISTEN)' not in listeners
                        or f'UDP 127.0.0.1:{args.udp_port}' not in listeners
                        or any('127.0.0.1:' not in row for row in listeners.splitlines()[1:])):
                    raise RuntimeError('Java listener 검증 실패: ' + listeners)
                data.update({'state': 'ready', 'login': 'LOGIN_OK', 'listeners': listeners,
                             'startup_log': (work / 'server.log').read_text(errors='replace')})
                save(ready, data)
                print(json.dumps({'pid': proc.pid, 'ready_file': str(ready), 'work_dir': str(work), 'login': 'LOGIN_OK'}), flush=True)
                break
            last_error = probe.stderr[-1000:]
            time.sleep(0.15)
        else:
            raise RuntimeError('LOGIN 준비 대기 실패: ' + last_error)
        if not args.smoke:
            while not stopping and proc.poll() is None:
                time.sleep(0.2)
    finally:
        forced = False
        if proc is not None:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=args.stop_timeout)
                except subprocess.TimeoutExpired:
                    forced = True
                    proc.kill()
                    proc.wait(timeout=5)
            data.update({'exit_code': proc.returncode, 'forced_kill': forced, 'pid_exited': not alive(proc.pid)})
            if not data['pid_exited']:
                raise RuntimeError('Java PID가 남아 있음: ' + str(proc.pid))
        data['state'] = 'stopped'
        data['cleaned'] = not args.keep_dir
        save(ready, data)
        if not args.keep_dir:
            shutil.rmtree(work)
        print(json.dumps({'state': 'stopped', 'pid': data['pid'], 'forced_kill': forced,
                          'cleaned': data['cleaned']}), flush=True)
        if forced:
            raise RuntimeError('SIGTERM 시간 초과: SIGKILL로 회수함 (정상 종료 아님)')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    start = sub.add_parser('run', help='foreground supervisor; 준비되면 JSON/PID 출력')
    start.add_argument('--source', default=DEFAULT_SOURCE, required=not DEFAULT_SOURCE,
                       help='원본 배포본 server 디렉터리(기본 $SCOUTER_SERVER_DIST)')
    start.add_argument('--java-home', default=DEFAULT_JDK)
    start.add_argument('--udp-port', type=port, default=16100)
    start.add_argument('--tcp-port', type=port, default=16100)
    start.add_argument('--udp-rcvbuf', type=int, default=4 * 1024 * 1024)
    start.add_argument('--jvm-arg', action='append', help='반복 가능; 지정하면 기본 -Xmx512m 대체')
    start.add_argument('--compress-xlog', action='store_true')
    start.add_argument('--compress-profile', action='store_true')
    start.add_argument('--start-timeout', type=float, default=10)
    start.add_argument('--stop-timeout', type=float, default=5)
    start.add_argument('--smoke', action='store_true', help='LOGIN 확인 직후 종료')
    start.add_argument('--keep-dir', action='store_true', help='종료 후 로그/DB 보존; cleanup으로 제거')
    for name in ('stop', 'cleanup'):
        p = sub.add_parser(name)
        p.add_argument('--ready-file', required=True)
    start.add_argument('--ready-file', required=True, help='존재하지 않는 외부 JSON 경로')
    args = parser.parse_args()
    if args.action == 'run':
        if args.start_timeout <= 0 or args.stop_timeout <= 0 or args.udp_rcvbuf <= 0:
            parser.error('timeout/udp-rcvbuf는 양수여야 함')
        run(args)
        return
    path = Path(args.ready_file).resolve()
    data = json.loads(path.read_text())
    if args.action == 'stop':
        if data['pid'] > 0 and owned(data):
            os.kill(data['pid'], signal.SIGTERM)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            data = json.loads(path.read_text())
            if data['state'] == 'stopped' and (data['pid'] == 0 or not alive(data['pid'])):
                print(json.dumps({k: data.get(k) for k in ('state', 'pid', 'pid_exited', 'forced_kill', 'cleaned')}))
                if data.get('forced_kill'):
                    raise RuntimeError('SIGKILL 종료: 정상 drain 미확인')
                return
            time.sleep(0.1)
        raise RuntimeError('종료 확인 시간 초과: supervisor/ready 파일 확인 필요')
    if data['state'] != 'stopped' or (data['pid'] > 0 and alive(data['pid'])):
        raise RuntimeError('실행 중이거나 종료 미확인: cleanup 거절')
    work = Path(data['work_dir'])
    if not work.name.startswith('scouter-java-bench-') or work.parent != Path(tempfile.gettempdir()).resolve():
        raise RuntimeError('임시 경로 검증 실패')
    if work.exists():
        shutil.rmtree(work)
    data['cleaned'] = True
    save(path, data)
    print('CLEANED ' + str(work))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
