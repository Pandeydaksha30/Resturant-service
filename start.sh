#!/bin/bash
# Food Delivery System - Startup Script

echo "🍕 Food Delivery System - Startup"
echo "=================================="

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Run migrations (if needed)
echo "🔧 Setting up database..."
python -c "from main import init_db; init_db()"

# Start the application
echo "🚀 Starting FastAPI server..."
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

echo "✅ Server is running at http://localhost:8000"
echo "📖 API Docs: http://localhost:8000/docs"
