FROM python:3.13.10-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

COPY "./pyproject.toml" "./uv.lock" "./.python-version" ./

RUN uv sync --locked

# Copy application code
COPY . .

CMD ["uv", "run", "python", "main.py"]