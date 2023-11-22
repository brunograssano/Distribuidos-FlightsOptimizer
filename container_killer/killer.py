import subprocess
import os

def kill_container():
    containers = input("Container names [Comma Separated Container Names]: ")
    containers = containers.split(",")
    for container in containers:
        result = subprocess.run(['docker','kill',container], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Kill executed on {container}. \nResult={result.returncode}. Output={result.stdout}. Error={result.stderr}")

if __name__ == '__main__':
    kill_container()