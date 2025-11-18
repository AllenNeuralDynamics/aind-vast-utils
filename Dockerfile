FROM python:3.12-slim
WORKDIR /app
ADD src ./src
ADD pyproject.toml .
ADD setup.py .

# Add git in case we need to install from branches
RUN apt-get update && apt-get install -y git
RUN pip install . --no-cache-dir
