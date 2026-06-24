# ════════════════════════════════════════════════════════════════
#  🔥 ZUDO USERBOT — VC FIGHT + MUSIC USERBOT  Dockerfile
# ════════════════════════════════════════════════════════════════
FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Kolkata \
    NODE_VERSION=20.x

# ─── System deps + Node.js 20 + ffmpeg ───────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        gcc g++ libc-dev libffi-dev libssl-dev \
        make git curl wget ca-certificates gnupg \
        tzdata \
        python3-dev \
        build-essential \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
       | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] \
       https://deb.nodesource.com/node_${NODE_VERSION} nodistro main" \
       > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update \
    && apt-get install -y nodejs \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Verify versions
RUN node --version && npm --version && ffmpeg -version | head -1

# ─── Working dir ─────────────────────────────────────────────────
WORKDIR /app

# ─── Python deps (cached layer) ─────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

# Force latest yt-dlp (extractors break weekly)
RUN pip install --no-cache-dir --upgrade yt-dlp

# ─── Bot code ────────────────────────────────────────────────────
COPY bot.py .

# ─── Directories with proper perms ───────────────────────────────
RUN mkdir -p /app/sessions /app/data /app/cookies /tmp/downloads \
 && chmod -R 777 /app/sessions /app/data /app/cookies /tmp/downloads

# ─── Healthcheck ─────────────────────────────────────────────────
HEALTHCHECK --interval=45s --timeout=10s --start-period=20s --retries=3 \
  CMD pgrep -f "python.*bot.py" > /dev/null || exit 1

# ─── Run ─────────────────────────────────────────────────────────
CMD ["python", "-u", "bot.py"]
