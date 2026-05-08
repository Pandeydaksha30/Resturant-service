# 🍕 Food Delivery System - Integrated Monolith

A production-ready **FastAPI application** integrating 9 microservice modules into a single deployable system.

## ✨ Features

### 9 Integrated Modules:
1. **User Module** - Registration & Authentication
2. **Payment Module** - Stripe PaymentIntent lifecycle
3. **Payment Service** - Idempotency, Circuit Breaker, Retries
4. **Payment Success** - Event publishing
5. **Payment Consumer** - Restaurant & Dispatch trigger
6. **Restaurant Service** - SQS/SNS queue simulation
7. **Dispatch Service** - Rider assignment with SSE
8. **Cloud Module** - Saga, Priority Queue, Rate Limiting
9. **Frontend Bridge** - React API routes

---

## 🚀 Quick Start

### Local Development

**Option 1: Direct Python (Recommended)**
```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
uvicorn main:app --reload --port 8000
```

**Option 2: Docker**
```bash
docker-compose up --build
```

**Option 3: Bash Script**
```bash
chmod +x start.sh
./start.sh
```

### 🌐 Access Your Application

- **Interactive API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health
- **Alternative Docs**: http://localhost:8000/redoc

---

## 🔧 Environment Configuration

Copy `.env.example` to `.env`:

```env
# Database
DATABASE_URL=sqlite:///./fooddelivery.db

# Security
SECRET_KEY=your-secret-key

# Stripe (optional)
STRIPE_SECRET_KEY=sk_test_xxx
STRIPE_WEBHOOK_SECRET=whsec_xxx

# Environment
ENVIRONMENT=development
```

---

## 📚 API Endpoints

### Authentication
- `POST /auth/register` - Register user
- `POST /auth/login` - User login
- `GET /users` - List users
- `GET /users/{user_id}` - Get user

### Payments
- `POST /payments/create` - Create payment
- `POST /payments/confirm` - Confirm payment
- `GET /payments/{id}` - Payment status

### Orders
- `POST /api/order/place` - Place order
- `GET /api/order/{id}/stream` - Stream updates (SSE)
- `GET /api/riders` - List riders

### System
- `GET /health` - Health check
- `GET /metrics` - System metrics
- `GET /events` - Event log

---

## 🌍 Deployment Options

### **Option 1: Render (Recommended - Free)**

1. Go to https://render.com
2. Click "New Web Service"
3. Connect GitHub repository
4. Configure:
   ```
   Runtime: Python 3.11
   Build: pip install -r requirements.txt
   Start: uvicorn main:app --host 0.0.0.0 --port 8000
   ```
5. Add environment variables and deploy

**Live URL**: `https://your-app.onrender.com`

---

### **Option 2: Railway**

1. Go to https://railway.app
2. Create new project from GitHub
3. Configure start command:
   ```
   uvicorn main:app --host 0.0.0.0 --port $PORT
   ```
4. Add PostgreSQL service
5. Deploy

---

### **Option 3: Heroku**

```bash
# Install Heroku CLI
curl https://cli-assets.heroku.com/install.sh | sh

# Login
heroku login

# Create app
heroku create your-app-name

# Add PostgreSQL
heroku addons:create heroku-postgresql:hobby-dev

# Deploy
git push heroku main
```

---

### **Option 4: AWS EC2**

```bash
# SSH into instance
ssh -i key.pem ec2-user@your-ip

# Clone and setup
git clone https://github.com/Pandeydaksha30/Resturant-service.git
cd Resturant-service
pip install -r requirements.txt

# Run with Gunicorn
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:8000 main:app
```

---

## 🐳 Docker Deployment

```bash
# Build image
docker build -t food-delivery:latest .

# Run container
docker run -p 8000:8000 \
  -e DATABASE_URL=sqlite:///./fooddelivery.db \
  -e SECRET_KEY=your-secret \
  food-delivery:latest
```

---

## 📊 Testing Endpoints

```bash
# Health check
curl http://localhost:8000/health

# Register user
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "username": "testuser",
    "password": "Password123"
  }'

# Place order
curl -X POST http://localhost:8000/api/order/place \
  -H "Content-Type: application/json" \
  -d '{
    "customer_name": "John Doe",
    "items": ["Pizza", "Coke"],
    "amount": 500.0
  }'
```

---

## 🔒 Security Best Practices

Before deploying to production:

1. **Change SECRET_KEY**:
   ```python
   import secrets
   print(secrets.token_urlsafe(32))
   ```

2. **Use PostgreSQL** instead of SQLite:
   ```env
   DATABASE_URL=postgresql://user:pass@host:5432/fooddelivery
   ```

3. **Enable HTTPS** (most hosting providers do this automatically)

4. **Set strong passwords** for all services

5. **Keep dependencies updated**:
   ```bash
   pip install --upgrade -r requirements.txt
   ```

---

## ⚙️ Database

### SQLite (Development)
```env
DATABASE_URL=sqlite:///./fooddelivery.db
```

### PostgreSQL (Production)
```env
DATABASE_URL=postgresql://user:password@localhost:5432/fooddelivery
```

---

## 🐛 Troubleshooting

### Port 8000 in use
```bash
lsof -i :8000
kill -9 <PID>
```

### Database errors
```bash
rm fooddelivery.db  # Reset database
# Restart app - it will recreate the database
```

### Missing dependencies
```bash
pip install -r requirements.txt --force-reinstall
```

---

## 📈 Performance Features

✅ **Circuit Breaker** - Automatic gateway failure handling
✅ **Idempotency** - Prevents duplicate payments
✅ **Rate Limiting** - 10 requests per user per 60 seconds
✅ **Event Bus** - Efficient inter-module communication
✅ **Connection Pooling** - Optimized database connections

---

## 📝 Project Structure

```
Resturant-service/
├── main.py              # All 9 modules integrated
├── requirements.txt     # Dependencies
├── dockerfile          # Docker configuration
├── docker-compose.yml  # Docker Compose
├── .env               # Local environment
├── .env.example       # Example environment
├── start.sh          # Startup script
└── README.md         # This file
```

---

## 🎓 Learning Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com)
- [Pydantic Documentation](https://docs.pydantic.dev)
- [SQLAlchemy ORM](https://docs.sqlalchemy.org)
- [Stripe API](https://stripe.com/docs/api)

---

## 📞 Support

**Issues?** Check the troubleshooting section or open a GitHub issue.

---

## 📄 License

MIT License - Open source and free to use

---

**Status**: ✅ Production Ready  
**Last Updated**: 2026-05-08  
**Version**: 1.0.0
