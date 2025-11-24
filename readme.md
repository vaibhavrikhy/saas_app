🚀 SaaS App – FastAPI × PostgreSQL × Docker

SaaS App is a modern backend platform built with FastAPI that demonstrates how scalable SaaS architectures handle secure authentication, authorization, and user management.
It integrates JWT, GitHub Login, Multi-Factor Authentication (MFA), and API Key Security, backed by PostgreSQL and Docker, offering a production-grade blueprint for full-stack SaaS systems.

✨ Features

🔐 JWT + OAuth2 Authentication

🧩 GitHub Login Integration

🧠 Multi-Factor Authentication (MFA)

🗝️ API Key Management

🧑‍💻 Role-Based Access Control (Admin, User, Premium)

💾 PostgreSQL Database with SQLAlchemy

🐳 Dockerized for Seamless Deployment

🧰 Swagger UI Documentation Built-In


⚙️ Quick Start
1️⃣ Clone the Repository
git clone https://github.com/vaibhavrikhy/SaasApp.git
cd SaasApp

2️⃣ Set Up Environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

3️⃣ Start PostgreSQL with Docker
docker-compose up -d

4️⃣ Run the App
uvicorn main:app --reload


Then visit 👉 http://127.0.0.1:8000/docs
 for Swagger UI.

🔑 Authentication Flow
Step	Endpoint	Description
1️⃣	/register/user	Create a new account
2️⃣	/token	Log in and get JWT token
3️⃣	/enable-mfa/{username}	Activate MFA (returns secret + QR)
4️⃣	/generate-api-key/{username}	Generate unique API key
5️⃣	/users/me	Fetch current authenticated user
🧩 Tech Stack
Layer	Technology
Framework	FastAPI (Python 3.12)
Database	PostgreSQL
ORM	SQLAlchemy
Auth	OAuth2 + JWT + PyOTP (MFA)
DevOps	Docker & Docker Compose
Testing	Pytest
💡 Example Endpoints
Route	Method	Access	Description
/register/user	POST	Public	Register new user
/token	POST	Public	Login and receive JWT
/users/me	GET	Authenticated	Get user profile
/admin/dashboard	GET	Admin Only	View admin panel
/premium/feature	GET	Premium	Premium-only features
🧠 Future Enhancements

Add Stripe billing integration

Add Redis for session management

Deploy on AWS (ECS or Lambda)

🧾 License

This project is released under the MIT License.
