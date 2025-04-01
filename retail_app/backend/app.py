# from gql import Client, gql
# from gql.transport.requests import RequestsHTTPTransport
import os
import random
import struct
from itertools import chain, repeat

# Try to import pyodbc, but don't fail if it's not available
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    print("Warning: pyodbc not available. Using mock data only.")
    PYODBC_AVAILABLE = False

import requests
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
from deltalake import DeltaTable
from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_caching import Cache  # Import Flask-Caching
from flask_cors import CORS

# Load environment variables
load_dotenv()

app = Flask(__name__)
# app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key")
CORS(app, origins=["http://localhost:3000"], supports_credentials=True)

# Configure Flask-Caching
cache_config = {
    "CACHE_TYPE": "SimpleCache",  # Use simple in-memory cache for PoC
    "CACHE_DEFAULT_TIMEOUT": 60*5  # Cache timeout in seconds (60 seconds = 1 minute)
}
cache = Cache(app, config=cache_config)

# Azure AD / Entra ID Configuration
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = [os.getenv("API_SCOPE")]

def _bytes2mswin_bstr(value: bytes) -> bytes:
    """Convert a sequence of bytes into a (MS-Windows) BSTR (as bytes).

    See https://github.com/mkleehammer/pyodbc/issues/228#issuecomment-319190980
    for the original code.  It appears the input is converted to an
    MS-Windows BSTR (in 'Little-endian' format).

    See https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp\
       /692a42a9-06ce-4394-b9bc-5d2a50440168
    for more info on BSTR.

    :param value: the sequence of bytes to convert
    :return: the converted value (as a sequence of bytes)
    """

    encoded_bytes = bytes(chain.from_iterable(zip(value, repeat(0))))
    return struct.pack("<i", len(encoded_bytes)) + encoded_bytes

# Set up the connection string
# 

def get_lakehouse_client():
    """Get a connection to the Azure SQL Lakehouse.
    
    Returns None if the required dependencies are not available.
    """

    try:
        credential = DefaultAzureCredential()
        databaseToken = credential.get_token('https://database.windows.net/')
        tokenstruct = _bytes2mswin_bstr(databaseToken.token.encode())

        conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={TDS_ENDPOINT};DATABASE={DATABASE};Encrypt=Yes;"

        return pyodbc.connect(conn_str, attrs_before={1256: tokenstruct})
    except Exception as e:
        raise e
        print(f"Error connecting to lakehouse: {str(e)}")
        return None
    


# Acquire a token
# DO NOT USE IN PRODUCTION.
# Below code to acquire token is for development purpose only to test the GraphQL endpoint
# For production, always register an application in a Microsoft Entra ID tenant and use the appropriate client_id and scopes
# https://learn.microsoft.com/en-us/fabric/data-engineering/connect-apps-api-graphql#create-a-microsoft-entra-app


# GraphQL API Configuration
GRAPHQL_URL = os.getenv("GRAPHQL_URL")
LAKEHOUSE_URL = os.getenv("LAKEHOUSE_URL")
TDS_ENDPOINT = os.getenv("TDS_ENDPOINT")
DATABASE = os.getenv("DATABASE")



# Mock data for PoC
mock_products = [
    {
        "id": f"product-{i}",
        "name": f"Brazilian {categories[i % len(categories)]} {i}",
        "description": "Experience the authentic taste of Brazil with this premium product. Made with the finest ingredients and traditional methods.",
        "price": round(random.uniform(20, 200), 2),
        "imageUrl": f"https://picsum.photos/400/300?random={i}",
        "category": categories[i % len(categories)],
        "rating": round(random.uniform(3, 5), 1),
        "stock": random.randint(0, 50),
        "details": "Our premium Brazilian product is sourced from sustainable farms in Brazil. Carefully crafted to perfection, this product reveals the true essence of Brazilian culture.\n\nMade in: Brazil\nWeight: 250g"
    }
    for i, categories in enumerate([
        "Coffee", "Crafts", "Food", "Beauty", "Fashion", "Home Decor", 
        "Coffee", "Crafts", "Food", "Beauty", "Fashion", "Home Decor",
        "Coffee", "Crafts", "Food", "Beauty", "Fashion", "Home Decor",
        "Coffee", "Crafts", "Food", "Beauty", "Fashion", "Home Decor"
    ])
]

def get_access_token():
    """Get an access token for the GraphQL API.
    
    Returns None if the required dependencies are not available or authentication fails.
    """
    try:
        app = InteractiveBrowserCredential()
        scp = 'https://analysis.windows.net/powerbi/api/user_impersonation'
        result = app.get_token(scp)
        
        if not result.token:
            print('Error:', "Could not get access token")
            return None
        else:
            return result.token
    except Exception as e:
        raise e

    
conn = get_lakehouse_client()
token = get_access_token()

def fetch_delta_table_data(table_name: str):
    """Fetch data from a Delta Lake table.
    
    Returns None if the required dependencies are not available or the fetch fails.
    """
    try:            
        # Get token for Azure Storage (not Fabric API)
        credential = DefaultAzureCredential()
        token = credential.get_token("https://storage.azure.com/.default").token
        storage_options = {
            "bearer_token": token,
            "use_fabric_endpoint": "true"
        }
        # Create DeltaTable instance with storage options
        delta_table = DeltaTable(f"{LAKEHOUSE_URL}/Tables/{table_name}", storage_options=storage_options)
        response = delta_table.to_pyarrow_table()
        # exclude products without category_name    
        return response.to_pylist()
    except Exception as e:
        print(f"Error fetching delta table data: {str(e)}")
        raise

def fetch_gql_data(query: str, variables: dict = None):
    """Fetch data from a GraphQL API.
    
    Returns None if the required dependencies are not available or the fetch fails.
    """
    try:
        if not token:
            print("Error: Could not get access token")
            return None
            
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.post(GRAPHQL_URL, json={'query': query, 'variables': variables}, 
                                headers=headers)
        return response.json()
    except Exception as e:
        print(f"Error fetching GraphQL data: {str(e)}")
        raise


@app.route('/api/products', methods=['GET', 'OPTIONS'])
def get_products():
    """Get all products from the Delta Lake table or mock data."""
    try:
        products = fetch_delta_table_data("Products")
        # exclude products without category_name
        products = [p for p in products if p.get("Category", None)]
        if products:
            return jsonify(products)
        else:
            print("No products returned from Delta Lake. Using mock data.")
            return jsonify(mock_products)
    except Exception as e:
        # Return mock data if API call fails
        print(f"API error: {str(e)}. Using mock product data for PoC")
        raise

@app.route('/api/products/<product_id>', methods=['GET'])
def get_product(product_id):
    """Get a specific product by ID or mock data."""
    try:
        cursor = conn.cursor()
        
        sql_query = "SELECT * FROM Products WHERE product_id = ?" 
        # print(sql_query)
        cursor.execute(sql_query, product_id)#"601a360bd2a916ecef0e88de72a6531a")
        # Get column names from cursor description
        columns = [column[0] for column in cursor.description]

        return jsonify(dict(zip(columns, cursor.fetchone())))

    except Exception as e:
        # Return mock data if API call fails
        print(f"API error: {str(e)}. Using mock product data for PoC")
        raise

@app.route('/api/categories', methods=['GET'])
def get_categories():
    """Get all categories from the Delta Lake table or mock data."""
    try:
        categories = fetch_delta_table_data("Product_Categories_Mapping")
        return jsonify(categories)
    except Exception as e:
        print(f"API error: {str(e)}. Using mock categories data for PoC")
        raise

@app.route('/api/recommendations/similar/<product_id>', methods=['GET'])
def get_similar_products(product_id):
    """Get similar product recommendations or mock data."""
    try:
        # Try to get products from Delta Lake for similar products
        products = fetch_delta_table_data("Products")
        if products:
            # Get 4 random products as similar products
            import random
            similar_products = random.sample(products, min(4, len(products)))
            return jsonify(similar_products)
        
        # Fall back to mock data
        print("Using mock similar products data for PoC")
        # Get 4 random products
        similar_products = random.sample(mock_products, min(4, len(mock_products)))
        return jsonify(similar_products)
    except Exception as e:
        # Return mock data if API call fails
        print(f"API error: {str(e)}. Using mock similar products data for PoC")
        raise

@app.route('/api/recommendations/personal/<user_id>', methods=['GET'])
def get_personal_recommendations(user_id):
    """Get personalized product recommendations or mock data."""
    try:
        query = """
        query
        ($customer_id: String!)
        {
        userProductRecommendations(first: 10, 
        orderBy:  {
           recommendation_ts_utc: DESC
        },
        filter:  {
        customer_id:  {
            startsWith: $customer_id
        },
        }
        ) {
        items {
            products {
               product_id
               Category
               Product_Segment
            }
        }
        } 
    } 
    """
        variables = {"customer_id": user_id[:-1]}
        result = fetch_gql_data(query, variables)
        return jsonify(result["data"]["userProductRecommendations"]["items"])

    except Exception as e:
        # Return mock data if API call fails
        print(f"API error: {str(e)}. Using mock recommendations data for PoC")
        raise e

if __name__ == '__main__':
    app.run(debug=True)


