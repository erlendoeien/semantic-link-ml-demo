# Brazilify - Fabric Workspaces and E-commerce Web Application

This repository contains a comprehensive e-commerce solution with Microsoft Fabric workspaces for data processing and machine learning, along with a modern web application for retail operations.

## Repository Structure

```
.
├── retail_app/          # Web application (React frontend + Flask backend)
├── ml/                  # Machine Learning workspaces and notebooks
└── raw/                 # Data ingestion and processing workspaces
```

## Components Overview

### 1. Web Application (`retail_app/`)
A modern e-commerce web application built with React and Flask.

#### Frontend
- React with TypeScript
- Material UI components
- Responsive design
- Product browsing and search
- Shopping cart functionality
- User profile management

#### Backend
- Flask API server
- GraphQL integration
- Microsoft Entra ID authentication
- RESTful endpoints

### 2. Machine Learning Workspaces (`ml/`)
Fabric workspaces for ML operations including:

- Product recommendation system
- Sales forecasting
- Defect product analysis
- High-level product categorization
- Monitoring and inference pipelines

Key ML Components:
- `Train - RecSys - Product recommendation.Notebook/`
- `SalesForecast - AutoML LowCode.Notebook/`
- `Reviews - Defect Product Analysis.Notebook/`
- `Inference - RecSys - Product recommendation.Notebook/`

### 3. Data Processing Workspaces (`raw/`)
Fabric workspaces for data ingestion and processing:

- Raw data lakehouse
- Translation pipelines
- Setup and environment configuration
- Data transformation workflows

## Getting Started

### Web Application Setup

1. Backend Setup:
   ```bash
   cd retail_app/backend
   source .venv/bin/activate
   uv pip install -r requirements.txt
   python app.py
   ```

2. Frontend Setup:
   ```bash
   cd retail_app/frontend
   npm install
   npm start
   ```

### Fabric Workspaces

The ML and data processing workspaces are configured in Microsoft Fabric. To use these:

1. Access the workspaces through Microsoft Fabric portal
2. Configure necessary permissions and connections
3. Run notebooks in sequence:
   - Start with `raw/Setup.Notebook`
   - Process data through the lakehouse
   - Execute ML training and inference pipelines

## Development Notes

- The web application is designed as a proof of concept
- ML models are trained using AutoML capabilities
- Data processing follows a lakehouse architecture
- All components are integrated with Microsoft Fabric

## Environment Requirements

- Node.js 16+ for frontend
- Python 3.8+ for backend
- Microsoft Fabric workspace access

## License

This project is intended as a proof of concept and is not licensed for production use. 