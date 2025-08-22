# ---- Base image for building and installing dependencies ----
FROM python:3.9-slim-buster AS builder

# Install Poetry 1.4.2 - most compatible with Python 3.9 and stable
RUN pip install poetry==1.4.2 \
    && poetry config virtualenvs.create false

# Set working directory for the application
WORKDIR /app

# Copy pyproject.toml and poetry.lock to the builder stage
COPY pyproject.toml poetry.lock ./

# Install dependencies using Poetry
# Poetry 1.4+ syntax: --only=main instead of --only main
RUN poetry install --only=main --no-root

# Export dependencies to requirements.txt for the final stage
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

# ---- Final image for runtime ----
FROM python:3.9-slim-buster AS runtime

# Set working directory
WORKDIR /app

# Copy only the necessary requirements.txt from the builder stage
COPY --from=builder /app/requirements.txt ./

# Install dependencies using pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy poetry config files
COPY pyproject.toml poetry.lock ./

# Copy the application code
COPY src ./src

# Copy the data directory
COPY data ./data

# Expose the port FastAPI listens on
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]