# Brazilian-Inspired E-commerce App (PoC)

A beautiful, minimal Brazilian-inspired e-commerce web application (similar to Olist). This is a Proof of Concept featuring a React frontend and Flask backend.

## Project Structure

```
retail_app/
├── backend/              # Flask backend
│   ├── .venv/            # Python virtual environment
│   ├── app.py            # Main Flask application
│   ├── .env.example      # Environment variables example
│   └── README.md         # Backend documentation
│
└── frontend/             # React frontend
    ├── public/           # Static files
    └── src/              # Source code
        ├── components/   # Reusable components
        ├── pages/        # Page components
        └── services/     # API services
```

## Features

- 🌐 Beautiful, responsive Brazilian-inspired UI
- 🛍️ Product browsing with filters and sorting
- 🔍 Product search functionality
- 📊 Product detail pages with recommendations
- 🛒 Shopping cart functionality (mock implementation)
- 👤 User profile (mock implementation)
- 📱 Fully responsive design for all devices

## Tech Stack

### Frontend
- React with TypeScript
- Material UI for beautiful components
- React Router for navigation
- Axios for API requests

### Backend
- Flask for API endpoints
- GraphQL client for data retrieval
- Microsoft Entra ID (Azure AD) authentication

## Getting Started

### Backend Setup

1. Navigate to the backend directory:
   ```
   cd retail_app/backend
   ```

2. Activate the virtual environment:
   ```
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install required packages:
   ```
   uv pip install flask flask-cors python-dotenv requests gql msal
   ```

4. Set up your environment variables:
   ```
   cp .env.example .env
   ```
   Then edit `.env` with your actual values for GraphQL API and Entra ID credentials.

5. Run the backend:
   ```
   python app.py
   ```
   The server will start on http://localhost:5000.

### Frontend Setup

1. Navigate to the frontend directory:
   ```
   cd retail_app/frontend
   ```

2. Install dependencies:
   ```
   npm install
   ```

3. Run the development server:
   ```
   npm start
   ```
   The frontend will be available at http://localhost:3000.

## Development Notes

This project is a Proof of Concept and includes:

- Mock data generation when API connections fail
- Simplified authentication flow
- Basic error handling

In a production environment, you would want to add:

- Proper error boundaries and fallbacks
- Comprehensive unit and integration tests
- State management with Redux or Context API
- Proper backend data persistence
- Complete authentication flow
- API caching and optimization

## License

This project is intended as a Proof of Concept and is not licensed for production use. 