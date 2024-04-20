import subprocess

# Execute the shell command to create a virtual environment
subprocess.run(["python3", "-m", "venv", "env"])

# Activate the virtual environment
activate_cmd = "source env/bin/activate"
subprocess.run(["bash", "-c", activate_cmd])

# Install the required package
subprocess.run(["pip", "install", "google-cloud-storage"])

# Now you can use the virtual environment
# Your code goes here...
# run producer
#wait 3 minutes
#run consumer.
# Deactivate the virtual environment when done
subprocess.run(["deactivate"])
