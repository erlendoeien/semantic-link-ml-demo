import React, { useState, useEffect } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import {
  Box,
  Container,
  Typography,
  Button,
  Grid,
  Paper,
  Divider,
  CircularProgress,
  useTheme,
  useMediaQuery,
} from '@mui/material';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import ProductCard from '../components/ProductCard';
import apiService, { Product } from '../services/api';

// Mock data for the hero banner (in a real app, this would come from the API)
const heroBannerData = {
  title: 'Discover Authentic Brazilian Products',
  description: 'Shop our curated collection of unique Brazilian products, from premium coffee to handcrafted home decor',
  buttonText: 'Shop Now',
  buttonLink: '/products',
  backgroundImage: 'https://images.unsplash.com/photo-1518639192441-8fce0a366e2e?ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D&auto=format&fit=crop&w=1740&q=80',
};

const HomePage: React.FC = () => {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));
  const isTablet = useMediaQuery(theme.breakpoints.between('sm', 'md'));
  
  const [featuredProducts, setFeaturedProducts] = useState<Product[]>([]);
  const [personalRecommendations, setPersonalRecommendations] = useState<Product[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  
  // Mock user ID - in a real app, this would come from authentication
  const mockUserId = '058b8118018cd37ff01ae5339a76c980';
  
  useEffect(() => {
    const fetchData = async () => {
      try {
        // Fetch products data
        const products = await apiService.getProducts();
        
        // For the PoC, simulate featured products by taking the first few
        setFeaturedProducts(
          products.slice(0, 8).map(product => ({
            ...product,
            isNew: Math.random() > 0.7,
            isOnSale: Math.random() > 0.8,
            isFavorite: Math.random() > 0.7,
          }))
        );
        
        // Fetch personalized recommendations
        try {
          const recommendations = await apiService.getPersonalRecommendations(mockUserId);
          setPersonalRecommendations(
            recommendations.map(product => ({
              ...product,
              isNew: Math.random() > 0.7,
              isOnSale: Math.random() > 0.8,
              isFavorite: Math.random() > 0.7,
            }))
          );
        } catch (err) {
          // For PoC, if API endpoint fails, use some products as recommendations
          console.error('Error fetching personalized recommendations:', err);
          setPersonalRecommendations(
            products.slice(3, 7).map(product => ({
              ...product,
              isNew: Math.random() > 0.7,
              isOnSale: Math.random() > 0.8,
              isFavorite: Math.random() > 0.7,
            }))
          );
        }
      } catch (error) {
        console.error('Error fetching data:', error);
        
        // For PoC, generate mock data if API fails
        const mockProducts: Product[] = Array.from({ length: 8 }, (_, i) => ({
          product_id: `product-${i + 1}`,
          name: `Brazilian Product ${i + 1}`,
          description: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed euismod, justo vel tincidunt ultricies, justo erat.',
          price: Math.floor(Math.random() * 200) + 20,
          imageUrl: `https://picsum.photos/400/300?random=${i}`,
          category: i % 3 === 0 ? 'Coffee' : i % 3 === 1 ? 'Crafts' : 'Food',
          rating: Math.random() * 2 + 3,
          stock: Math.floor(Math.random() * 50),
          isNew: Math.random() > 0.7,
          isOnSale: Math.random() > 0.8,
          isFavorite: Math.random() > 0.7,
        }));
        
        setFeaturedProducts(mockProducts);
        setPersonalRecommendations(mockProducts.slice(4).concat(mockProducts.slice(0, 4)));
      } finally {
        setLoading(false);
      }
    };
    
    fetchData();
  }, []);
  
  const handleAddToCart = async (product_id: string) => {
    await apiService.addToCart(product_id);
  };
  
  const handleToggleFavorite = async (product_id: string) => {
    await apiService.toggleFavorite(product_id);
    
    // Update local state - in a real app, we'd refetch or use optimistic updates
    setFeaturedProducts(prevProducts =>
      prevProducts.map(product =>
        product.product_id === product_id
          ? { ...product, isFavorite: !product.isFavorite }
          : product
      )
    );
    
    setPersonalRecommendations(prevProducts =>
      prevProducts.map(product =>
        product.product_id === product_id
          ? { ...product, isFavorite: !product.isFavorite }
          : product
      )
    );
  };
  
  // Determine products per row based on screen size
  const getProductCols = () => {
    if (isMobile) return 1;
    if (isTablet) return 2;
    return 4;
  };
  
  return (
    <Box>
      {/* Hero Banner */}
      <Paper
        sx={{
          position: 'relative',
          height: { xs: '60vh', sm: '70vh', md: '80vh' },
          maxHeight: '800px',
          color: 'white',
          mb: 6,
          backgroundSize: 'cover',
          backgroundRepeat: 'no-repeat',
          backgroundPosition: 'center',
          backgroundImage: `linear-gradient(rgba(0, 0, 0, 0.3), rgba(0, 0, 0, 0.6)), url('${heroBannerData.backgroundImage}')`,
          display: 'flex',
          alignItems: 'center',
        }}
      >
        <Container maxWidth="lg">
          <Box sx={{ maxWidth: { xs: '100%', md: '60%' } }}>
            <Typography
              component="h1"
              variant="h2"
              color="inherit"
              gutterBottom
              sx={{
                fontWeight: 700,
                fontSize: { xs: '2.5rem', sm: '3rem', md: '3.75rem' },
                textShadow: '2px 2px 4px rgba(0,0,0,0.5)',
              }}
            >
              {heroBannerData.title}
            </Typography>
            <Typography
              variant="h5"
              color="inherit"
              paragraph
              sx={{
                mb: 4,
                textShadow: '1px 1px 3px rgba(0,0,0,0.5)',
                fontSize: { xs: '1rem', sm: '1.25rem', md: '1.5rem' },
              }}
            >
              {heroBannerData.description}
            </Typography>
            <Button
              component={RouterLink}
              to={heroBannerData.buttonLink}
              variant="contained"
              color="primary"
              size="large"
              endIcon={<ArrowForwardIcon />}
              sx={{
                fontSize: { xs: '1rem', md: '1.1rem' },
                px: 4,
                py: { xs: 1, md: 1.5 },
              }}
            >
              {heroBannerData.buttonText}
            </Button>
          </Box>
        </Container>
      </Paper>

      <Container maxWidth="lg">
        {/* Featured Products Section */}
        <Box sx={{ mb: 8 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 4 }}>
            <Typography variant="h4" component="h2" fontWeight="bold">
              Featured Products
            </Typography>
            <Button
              component={RouterLink}
              to="/products"
              endIcon={<ArrowForwardIcon />}
              color="primary"
            >
              View All
            </Button>
          </Box>

          {loading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 8 }}>
              <CircularProgress />
            </Box>
          ) : (
            <Grid container spacing={3}>
              {featuredProducts.slice(0, 8).map((product) => (
                <Grid item xs={12} sm={6} md={3} key={product.product_id}>
                  <ProductCard
                    {...product}
                    onAddToCart={handleAddToCart}
                    onToggleFavorite={handleToggleFavorite}
                  />
                </Grid>
              ))}
            </Grid>
          )}
        </Box>

        <Divider sx={{ mb: 8 }} />

        {/* Personalized Recommendations Section */}
        <Box sx={{ mb: 8 }}>
          <Typography variant="h4" component="h2" fontWeight="bold" sx={{ mb: 4 }}>
            Recommended For You
          </Typography>

          {loading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 8 }}>
              <CircularProgress />
            </Box>
          ) : (
            <Grid container spacing={3}>
              {personalRecommendations.slice(0, getProductCols() * 2).map((product) => (
                <Grid item xs={12} sm={6} md={3} key={product.product_id}>
                  <ProductCard
                    {...product}
                    onAddToCart={handleAddToCart}
                    onToggleFavorite={handleToggleFavorite}
                  />
                </Grid>
              ))}
            </Grid>
          )}
        </Box>

        {/* Categories Showcase */}
        <Box sx={{ mb: 8 }}>
          <Typography variant="h4" component="h2" fontWeight="bold" sx={{ mb: 4 }}>
            Shop by Category
          </Typography>

          <Grid container spacing={3}>
            {['Coffee', 'Crafts', 'Food', 'Beauty'].map((category, index) => (
              <Grid item xs={12} sm={6} md={3} key={category}>
                <Paper
                  component={RouterLink}
                  to={`/products?category=${category.toLowerCase()}`}
                  sx={{
                    height: 150,
                    display: 'flex',
                    flexDirection: 'column',
                    justifyContent: 'center',
                    alignItems: 'center',
                    textDecoration: 'none',
                    backgroundSize: 'cover',
                    backgroundPosition: 'center',
                    backgroundImage: `linear-gradient(rgba(0, 0, 0, 0.3), rgba(0, 0, 0, 0.6)), url('https://picsum.photos/400/300?random=${index + 10}')`,
                    color: 'white',
                    borderRadius: 2,
                    transition: 'transform 0.2s ease-in-out',
                    '&:hover': {
                      transform: 'scale(1.03)',
                    },
                  }}
                >
                  <Typography variant="h5" component="h3" fontWeight="bold">
                    {category}
                  </Typography>
                </Paper>
              </Grid>
            ))}
          </Grid>
        </Box>
      </Container>
    </Box>
  );
};

export default HomePage; 