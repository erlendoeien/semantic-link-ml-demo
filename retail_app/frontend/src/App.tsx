import React, { useEffect, useState } from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';

// Components
import Header from './components/Header';
import Footer from './components/Footer';
import HomePage from './pages/HomePage';
import ProductPage from './pages/ProductPage';
import ProductDetailPage from './pages/ProductDetailPage';
import NotFoundPage from './pages/NotFoundPage';
import apiService, { Category } from './services/api';

// Create a Brazilian-inspired theme
const theme = createTheme({
  palette: {
    primary: {
      main: '#00A86B', // Green from Brazilian flag
      light: '#4CD7A2',
      dark: '#007849',
      contrastText: '#fff',
    },
    secondary: {
      main: '#FDDF00', // Yellow from Brazilian flag
      light: '#FFEB7F',
      dark: '#D4B600',
      contrastText: '#000',
    },
    error: {
      main: '#D32F2F',
    },
    background: {
      default: '#f9f9f9',
      paper: '#fff',
    },
  },
  typography: {
    fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
    h1: {
      fontWeight: 700,
    },
    h2: {
      fontWeight: 600,
    },
    h3: {
      fontWeight: 600,
    },
    button: {
      fontWeight: 600,
      textTransform: 'none',
    },
  },
  shape: {
    borderRadius: 8,
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: 30,
          padding: '10px 24px',
        },
        contained: {
          boxShadow: '0px 4px 10px rgba(0, 0, 0, 0.15)',
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          boxShadow: '0px 2px 10px rgba(0, 0, 0, 0.08)',
          borderRadius: 12,
        },
      },
    },
  },
});

function App() {
  const [categories, setCategories] = useState<Category[]>([]);
  useEffect(() => {
    apiService.getCategories().then(setCategories);
  }, []);
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Router>
        <Header categories={categories.length > 0 
          ? [...categories]
              .sort(() => 0.5 - Math.random())
              .slice(0, 3)
              .map(c => c.original_product_category.toLocaleUpperCase())
          : []} />
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/products" element={<ProductPage />} />
          <Route path="/products/:id" element={<ProductDetailPage />} />
          <Route path="*" element={<NotFoundPage />} />
        </Routes>
        <Footer />
      </Router>
    </ThemeProvider>
  );
}

export default App;
