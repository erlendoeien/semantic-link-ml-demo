import React, { useState, useEffect } from 'react';
import { useParams, Link as RouterLink } from 'react-router-dom';
import {
  Box,
  Container,
  Typography,
  Grid,
  Paper,
  Divider,
  Button,
  Breadcrumbs,
  Link,
  CircularProgress,
  Rating,
  Tabs,
  Tab,
  Chip,
  TextField,
  IconButton,
  useMediaQuery,
  useTheme,
} from '@mui/material';
import ShoppingCartIcon from '@mui/icons-material/ShoppingCart';
import FavoriteIcon from '@mui/icons-material/Favorite';
import FavoriteBorderIcon from '@mui/icons-material/FavoriteBorder';
import ShareIcon from '@mui/icons-material/Share';
import AddIcon from '@mui/icons-material/Add';
import RemoveIcon from '@mui/icons-material/Remove';
import ProductCard from '../components/ProductCard';
import apiService, { Product } from '../services/api';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

const TabPanel: React.FC<TabPanelProps> = (props) => {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`product-tabpanel-${index}`}
      aria-labelledby={`product-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ py: 3 }}>{children}</Box>}
    </div>
  );
};

const ProductDetailPage: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('md'));
  
  const [product, setProduct] = useState<Product | null>(null);
  const [similarProducts, setSimilarProducts] = useState<Product[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [quantity, setQuantity] = useState<number>(1);
  const [isFavorite, setIsFavorite] = useState<boolean>(false);
  const [tabValue, setTabValue] = useState<number>(0);
  
  useEffect(() => {
    const fetchData = async () => {
      if (!id) return;
      
      setLoading(true);
      try {
        // Fetch product details
        const productData = await apiService.getProductById(id);
        setProduct({ ...productData, isFavorite: Math.random() > 0.5 });
        setIsFavorite(productData.isFavorite || false);
        
        // Fetch similar products
        try {
          const similar = await apiService.getSimilarProducts(id);
          setSimilarProducts(
            similar.map(p => ({
              ...p,
              isNew: Math.random() > 0.7,
              isOnSale: Math.random() > 0.8,
              isFavorite: Math.random() > 0.7,
            }))
          );
        } catch (err) {
          console.error('Error fetching similar products:', err);
          // Mock similar products for the PoC
          const mockSimilar: Product[] = Array.from({ length: 4 }, (_, i) => ({
            product_id: `similar-${i + 1}`,
            name: `Similar Product ${i + 1}`,
            description: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.',
            price: Math.floor(Math.random() * 200) + 20,
            imageUrl: `https://picsum.photos/400/300?random=${i + 20}`,
            category: productData.category,
            rating: Math.random() * 2 + 3,
            stock: Math.floor(Math.random() * 50),
            isNew: Math.random() > 0.7,
            isOnSale: Math.random() > 0.8,
            isFavorite: Math.random() > 0.7,
          }));
          setSimilarProducts(mockSimilar);
        }
      } catch (error) {
        console.error('Error fetching product details:', error);
        // Create mock data for the PoC
        const mockProduct: Product = {
          product_id: id || 'mock-product',
          name: 'Brazilian Arabica Coffee - Premium Blend',
          description: 'Experience the rich, smooth taste of authentic Brazilian coffee. This premium blend offers the perfect balance of flavor and aroma that will transport you to the coffee plantations of Brazil.',
          price: 29.99,
          imageUrl: 'https://picsum.photos/600/400?random=99',
          category: 'Coffee',
          rating: 4.5,
          stock: 25,
          details: 'Our premium Brazilian coffee is sourced from sustainable farms in the highlands of Brazil. Carefully roasted to perfection, this medium-dark roast reveals notes of chocolate, caramel, and a hint of citrus.\n\nIngredients: 100% Arabica coffee beans\nOrigin: Brazil\nRoast Level: Medium-Dark\nWeight: 250g',
          isNew: true,
          isOnSale: false,
          isFavorite: false,
        };
        setProduct(mockProduct);
        
        // Mock similar products
        const mockSimilar: Product[] = Array.from({ length: 4 }, (_, i) => ({
          product_id: `similar-${i + 1}`,
          name: `Similar Coffee Product ${i + 1}`,
          description: 'Another fantastic Brazilian coffee product that you might enjoy.',
          price: Math.floor(Math.random() * 40) + 20,
          imageUrl: `https://picsum.photos/400/300?random=${i + 20}`,
          category: 'Coffee',
          rating: Math.random() * 2 + 3,
          stock: Math.floor(Math.random() * 50),
          isNew: Math.random() > 0.7,
          isOnSale: Math.random() > 0.8,
          isFavorite: Math.random() > 0.7,
        }));
        setSimilarProducts(mockSimilar);
      } finally {
        setLoading(false);
      }
    };
    
    fetchData();
  }, [id]);
  
  const handleQuantityChange = (amount: number) => {
    const newQuantity = Math.max(1, quantity + amount);
    setQuantity(newQuantity);
  };
  
  const handleAddToCart = async () => {
    if (!product) return;
    await apiService.addToCart(product.product_id, quantity);
  };
  
  const handleToggleFavorite = async () => {
    if (!product) return;
    await apiService.toggleFavorite(product.product_id);
    setIsFavorite(!isFavorite);
  };
  
  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };
  
  const handleSimilarProductToggleFavorite = async (productId: string) => {
    await apiService.toggleFavorite(productId);
    
    // Update local state for similar products
    setSimilarProducts(prevProducts =>
      prevProducts.map(product =>
        product.product_id === productId
          ? { ...product, isFavorite: !product.isFavorite }
          : product
      )
    );
  };
  
  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', py: 8 }}>
        <CircularProgress />
      </Box>
    );
  }
  
  if (!product) {
    return (
      <Container maxWidth="lg" sx={{ py: 8 }}>
        <Typography variant="h5" align="center">
          Product not found
        </Typography>
      </Container>
    );
  }
  
  return (
    <Container maxWidth="lg" sx={{ py: 4 }}>
      <Breadcrumbs sx={{ mb: 3 }}>
        <Link component={RouterLink} to="/" color="inherit">
          Home
        </Link>
        <Link component={RouterLink} to="/products" color="inherit">
          Products
        </Link>
        <Link 
          component={RouterLink} 
          to={`/products?category=${product.category?.toLowerCase()}`} 
          color="inherit"
        >
          {product.category}
        </Link>
        <Typography color="text.primary">{product.name}</Typography>
      </Breadcrumbs>
      
      <Grid container spacing={4}>
        {/* Product Image */}
        <Grid item xs={12} md={6}>
          <Paper 
            elevation={1} 
            sx={{ 
              p: 2, 
              height: '100%', 
              display: 'flex', 
              alignItems: 'center', 
              justifyContent: 'center',
              position: 'relative',
            }}
          >
            {(product.isNew || product.isOnSale) && (
              <Box sx={{ position: 'absolute', top: 16, left: 16, zIndex: 1 }}>
                {product.isNew && (
                  <Chip 
                    label="NEW" 
                    color="primary" 
                    size="small" 
                    sx={{ mb: 1, fontWeight: 'bold' }} 
                  />
                )}
                {product.isOnSale && (
                  <Chip 
                    label="SALE" 
                    color="error" 
                    size="small" 
                    sx={{ fontWeight: 'bold' }} 
                  />
                )}
              </Box>
            )}
            <img 
              src={product.imageUrl} 
              alt={product.name} 
              style={{ 
                width: '100%', 
                height: 'auto', 
                maxHeight: 400, 
                objectFit: 'contain',
              }} 
            />
          </Paper>
        </Grid>
        
        {/* Product Details */}
        <Grid item xs={12} md={6}>
          <Box>
            <Typography variant="h4" component="h1" gutterBottom fontWeight="bold">
              {product.name}
            </Typography>
            
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
              <Rating value={product.rating} precision={0.5} readOnly />
              <Typography variant="body2" color="text.secondary" sx={{ ml: 1 }}>
                ({product.rating?.toFixed(1)})
              </Typography>
            </Box>
            
            <Typography variant="h5" color="primary" fontWeight="bold" sx={{ mb: 2 }}>
              R$ {product.price?.toFixed(2)}
            </Typography>
            
            <Typography variant="body1" paragraph>
              {product.description}
            </Typography>
            
            <Box sx={{ mb: 3 }}>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Category: {product.category}
              </Typography>
              <Typography 
                variant="body2" 
                color={product.stock && product.stock > 0 ? 'success.main' : 'error.main'} 
                gutterBottom
              >
                {product.stock && product.stock > 0 
                  ? `In Stock (${product.stock} available)` 
                  : 'Out of Stock'
                }
              </Typography>
            </Box>
            
            <Divider sx={{ my: 3 }} />
            
            {/* Quantity Selector */}
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
              <Typography variant="body1" sx={{ mr: 2 }}>
                Quantity:
              </Typography>
              <IconButton 
                onClick={() => handleQuantityChange(-1)} 
                disabled={quantity <= 1}
                size="small"
                sx={{ border: 1, borderColor: 'divider' }}
              >
                <RemoveIcon fontSize="small" />
              </IconButton>
              <TextField
                value={quantity}
                inputProps={{ 
                  readOnly: true,
                  sx: { textAlign: 'center', width: 30 },
                }}
                variant="standard"
                size="small"
                sx={{ mx: 1 }}
              />
              <IconButton 
                onClick={() => handleQuantityChange(1)} 
                disabled={quantity >= (product.stock ?? 0)}
                size="small"
                sx={{ border: 1, borderColor: 'divider' }}
              >
                <AddIcon fontSize="small" />
              </IconButton>
            </Box>
            
            {/* Action Buttons */}
            <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
              <Button
                variant="contained"
                color="primary"
                size="large"
                startIcon={<ShoppingCartIcon />}
                onClick={handleAddToCart}
                disabled={product.stock === 0}
                sx={{ flexGrow: 1, minWidth: 200 }}
              >
                Add to Cart
              </Button>
              <Button
                variant="outlined"
                color="primary"
                onClick={handleToggleFavorite}
                startIcon={isFavorite ? <FavoriteIcon color="error" /> : <FavoriteBorderIcon />}
              >
                {isFavorite ? 'Saved' : 'Save'}
              </Button>
              <Button
                variant="outlined"
                color="primary"
                startIcon={<ShareIcon />}
              >
                Share
              </Button>
            </Box>
          </Box>
        </Grid>
      </Grid>
      
      {/* Product Tabs */}
      <Paper sx={{ mt: 6, mb: 6 }} elevation={1}>
        <Tabs
          value={tabValue}
          onChange={handleTabChange}
          variant={isMobile ? 'scrollable' : 'fullWidth'}
          scrollButtons={isMobile ? 'auto' : false}
          sx={{ borderBottom: 1, borderColor: 'divider' }}
        >
          <Tab label="Details" />
          <Tab label="Specifications" />
          <Tab label="Reviews" />
          <Tab label="Shipping" />
        </Tabs>
        
        <Box sx={{ p: 3 }}>
          <TabPanel value={tabValue} index={0}>
            <Typography variant="body1" sx={{ whiteSpace: 'pre-line' }}>
              {product.details || 'Product details not available.'}
            </Typography>
          </TabPanel>
          
          <TabPanel value={tabValue} index={1}>
            <Grid container spacing={2}>
              {product.product_category_name && (
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">Category</Typography>
                  <Typography variant="body1">{product.product_category_name}</Typography>
                </Grid>
              )}
              {product.product_weight_g && (
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">Weight</Typography>
                  <Typography variant="body1">{product.product_weight_g}g</Typography>
                </Grid>
              )}
              {product.product_height_cm && (
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">Height</Typography>
                  <Typography variant="body1">{product.product_height_cm} cm</Typography>
                </Grid>
              )}
              {product.product_width_cm && (
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">Width</Typography>
                  <Typography variant="body1">{product.product_width_cm} cm</Typography>
                </Grid>
              )}
              {product.product_length_cm && (
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">Length</Typography>
                  <Typography variant="body1">{product.product_length_cm} cm</Typography>
                </Grid>
              )}
              {product.product_photos_qty && (
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">Number of Photos</Typography>
                  <Typography variant="body1">{product.product_photos_qty}</Typography>
                </Grid>
              )}
              {!product.product_height_cm && !product.product_width_cm && 
               !product.product_length_cm && !product.product_weight_g && 
               !product.product_photos_qty && !product.product_category_name && (
                <Grid item xs={12}>
                  <Typography variant="body1">
                    Product specifications not available.
                  </Typography>
                </Grid>
              )}
            </Grid>
          </TabPanel>
          
          <TabPanel value={tabValue} index={2}>
            <Typography variant="body1">
              Customer reviews would be displayed here.
            </Typography>
          </TabPanel>
          
          <TabPanel value={tabValue} index={3}>
            <Typography variant="body1">
              Shipping and return policy information would be shown here.
            </Typography>
          </TabPanel>
        </Box>
      </Paper>
      
      {/* Similar Products */}
      <Box sx={{ mb: 6 }}>
        <Typography variant="h5" component="h2" fontWeight="bold" sx={{ mb: 3 }}>
          You Might Also Like
        </Typography>
        
        <Grid container spacing={3}>
          {similarProducts.map((product) => (
            <Grid item xs={12} sm={6} md={3} key={product.product_id}>
              <ProductCard
                {...product}
                onAddToCart={apiService.addToCart}
                onToggleFavorite={handleSimilarProductToggleFavorite}
              />
            </Grid>
          ))}
        </Grid>
      </Box>
    </Container>
  );
};

export default ProductDetailPage; 