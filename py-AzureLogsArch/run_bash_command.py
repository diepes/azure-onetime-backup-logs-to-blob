import subprocess
import time

def run_command(command):
    start_time = time.time()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    end_time = time.time()
    duration = end_time - start_time

    return output.decode('utf-8'), error.decode('utf-8'), duration

command = "ls -l"
output, error, duration = run_command(command)
print("Output: ")
print(output)
print("Error: ")
print(error)
print(f"Command took {duration:.2f} seconds to execute")