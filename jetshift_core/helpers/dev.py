import os
import signal
import subprocess
import socket
from dotenv import load_dotenv

from jetshift_core.helpers.common import jprint

load_dotenv()

# Environment variables
app_env = os.environ.get('APP_ENV', 'development')
app_port = os.environ.get('APP_PORT', 8080)
luigi_port = os.environ.get('LUIGI_PORT', 8082)
cron_job = os.environ.get('CRON_JOB', False)
job_queue = os.environ.get('JOB_QUEUE', False)
redis_ssl = os.environ.get('REDIS_SSL', False)
redis_host = os.environ.get('REDIS_HOST')
redis_port = os.environ.get('REDIS_PORT', 6379)
redis_password = os.environ.get('REDIS_PASSWORD')


def find_process_using_port(port):
    result = subprocess.run(['lsof', '-i', f':{port}'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    lines = result.stdout.splitlines()
    if len(lines) > 1:
        process_info = lines[1].split()
        pid = int(process_info[1])
        return pid
    return None


def kill_process(pid):
    try:
        os.kill(pid, signal.SIGKILL)
        print(f"Process {pid} terminated")
    except OSError as e:
        jprint(f"Error terminating process {pid}: {e}", 'error')


def is_port_open(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    try:
        s.connect((host, port))
        s.close()
        return True
    except (socket.timeout, ConnectionRefusedError):
        return False


def start_subprocess(command, description, port=None, reset_port=False):
    try:
        print(f"Starting {description}")

        if port:
            port_open = is_port_open('localhost', int(port))
            if port_open:
                print(f"Port {port} is already in use" + (" and will be reset" if reset_port else "") + "\n")

                pid = find_process_using_port(port)
                if pid and reset_port:
                    print(f"Kill process {pid}\n")
                    kill_process(pid)

                return None

        open_process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        # open_process = subprocess.Popen(command)
        print(f"Started {description} on port {port}\n")
        return open_process
    except FileNotFoundError as e:
        jprint(f"Command not found: {command[0]}", 'error')
    except subprocess.SubprocessError as e:
        jprint(f"Failed to start {description}: {e}", 'error')
        return None


def dev_env(background=False, reset_port=False):
    processes = []

    start_cron_job = str(cron_job).strip().lower() == 'true'
    start_job_queue = str(job_queue).strip().lower() == 'true'
    check_redis_ssl = str(redis_ssl).strip().lower() == 'true'

    try:
        # Start cron
        if start_cron_job:
            process = start_subprocess(['crond', '-f'], 'cron')
            if process:
                processes.append(process)

        # Start luigid
        process = start_subprocess(['luigid', '--port', str(luigi_port)], 'luigid', int(luigi_port), reset_port)
        if process:
            processes.append(process)

        # Start the Flask app with Waitress
        if app_env == 'development':
            process = start_subprocess(['flask', 'run', '--host=0.0.0.0', '--port', str(app_port), '--reload'], 'web app', int(app_port), reset_port)
        else:
            process = start_subprocess(['waitress-serve', '--host=0.0.0.0', '--port', str(app_port), 'web.main:app'], 'web app', int(app_port), reset_port)
        if process:
            processes.append(process)

        # Start RQ worker
        if start_job_queue:
            redis_protocol = 'rediss' if check_redis_ssl else 'redis'

            if check_redis_ssl:
                redis_url = f"{redis_protocol}://:{redis_password}@{redis_host}:{redis_port}"
            else:
                redis_url = f"{redis_protocol}://{redis_host}:{redis_port}"

            process = start_subprocess(['rq', 'worker', '--url', redis_url], 'RQ SSL worker' if check_redis_ssl else 'RQ worker', int(redis_port), reset_port)
            if process:
                processes.append(process)

        # Wait for all background processes to finish
        if not background:
            for process in processes:
                process.wait()

    except KeyboardInterrupt:
        print("Terminating all subprocesses...")
        for process in processes:
            process.terminate()
        for process in processes:
            process.wait()
        print("All subprocesses terminated.")


def close_all_ports():
    try:
        ports = [app_port, luigi_port, redis_port]

        for port in ports:
            port_open = is_port_open('localhost', int(port))
            if port_open:
                pid = find_process_using_port(port)
                if pid:
                    kill_process(pid)

        jprint("All running ports closed.", 'success')
    except Exception as e:
        jprint(f"An error occurred: {e}", 'error')


if __name__ == '__main__':
    dev_env(background=False, reset_port=False)
