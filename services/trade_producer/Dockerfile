# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory in the container
WORKDIR /app

#set PYTHONPATH explicitly in the Dockerfile to ensure /app is included
ENV PYTHONPATH=/app

# Install Python Poetry with version 1.8.3
RUN pip install poetry==1.8.3

# Disable virtual environments for Poetry to install directly in the container's Python environment
ENV POETRY_VIRTUALENVS_CREATE=false

# Copy only the Poetry files to leverage Docker's cache for dependencies
COPY pyproject.toml poetry.lock /app/

# Install dependencies
RUN poetry install --no-root

# Copy the source code into the container
COPY . /app

# Run the application
CMD ["poetry", "run", "python", "src/main.py"]
