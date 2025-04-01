# Brazilian-Inspired E-commerce Backend

A Flask-based backend for the Brazilian-inspired e-commerce PoC application, integrating with a GraphQL API authenticated via Entra ID.

## Setup

1. Make sure you have Python installed
2. Install the required packages:
   ```
   uv pip install flask flask-cors python-dotenv requests gql msal
   ```
3. Copy `.env.example` to `.env` and fill in your actual configuration values:
   ```
   cp .env.example .env
   ```
4. Edit the `.env` file with your GraphQL API endpoint and Entra ID credentials

## Running the Application

```
python app.py
```

The server will start on http://localhost:5000.

## API Endpoints

- `GET /api/products` - Get all products
- `GET /api/products/<product_id>` - Get details for a specific product
- `GET /api/recommendations/similar/<product_id>` - Get similar product recommendations
- `GET /api/recommendations/personal/<user_id>` - Get personalized product recommendations for a user 