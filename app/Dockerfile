FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

# Install app dependencies (playwright + chromium already in base image)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py automation.py config.py notify.py nse.json ./

RUN groupadd -r app && useradd -r -g app app
USER app

EXPOSE 8080

# --timeout 30          : per-request timeout (webhook returns in <1 s)
# --graceful-timeout 300: time gunicorn gives worker to drain background
#                         threads after SIGTERM before sending SIGKILL
# --keep-alive 65       : Cloud Run load-balancer keep-alive is 620 s;
#                         65 s on the app side avoids RST races
CMD ["python", "-m", "gunicorn", "main:app", \
     "--bind", "0.0.0.0:8080", \
     "--workers", "1", \
     "--timeout", "30", \
     "--graceful-timeout", "300", \
     "--keep-alive", "65", \
     "--log-level", "info"]
