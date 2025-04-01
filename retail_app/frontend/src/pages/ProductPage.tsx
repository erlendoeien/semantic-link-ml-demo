import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import {
  Box,
  Container,
  Typography,
  Grid,
  Paper,
  Divider,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  Slider,
  Checkbox,
  FormGroup,
  FormControlLabel,
  CircularProgress,
  Breadcrumbs,
  Link,
  Pagination,
  SelectChangeEvent,
  IconButton,
  Drawer,
  Button,
  useMediaQuery,
  useTheme,
} from '@mui/material';
import { Link as RouterLink } from 'react-router-dom';
import FilterListIcon from '@mui/icons-material/FilterList';
import CloseIcon from '@mui/icons-material/Close';
import ProductCard from '../components/ProductCard';
import apiService, { Product } from '../services/api';

const sortOptions = [
  { value: 'newest', label: 'Newest Arrivals' },
  { value: 'price_asc', label: 'Price: Low to High' },
  { value: 'price_desc', label: 'Price: High to Low' },
  { value: 'rating', label: 'Customer Rating' },
];

const ProductPage: React.FC = () => {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('md'));
  const [searchParams, setSearchParams] = useSearchParams();
  
  // Filter states
  const [products, setProducts] = useState<Product[]>([]);
  const [filteredProducts, setFilteredProducts] = useState<Product[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [sortBy, setSortBy] = useState<string>(searchParams.get('sort') || 'newest');
  const [priceRange, setPriceRange] = useState<number[]>([0, 1000]);
  const [category, setCategory] = useState<string>(searchParams.get('category') || 'All Categories');
  const [categoryOptions, setCategoryOptions] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>(searchParams.get('q') || '');
  const [inStockOnly, setInStockOnly] = useState<boolean>(false);
  const [filterDrawerOpen, setFilterDrawerOpen] = useState<boolean>(false);
  
  // Pagination
  const [page, setPage] = useState<number>(1);
  const productsPerPage = 12;
  
  useEffect(() => {
    const fetchProducts = async () => {
      try {
        setLoading(true);
        const data = await apiService.getProducts();
        
        // Add some random properties for the demo
        const productsWithExtras = data.map(product => ({
          ...product,
          isNew: Math.random() > 0.7,
          isOnSale: Math.random() > 0.8,
          isFavorite: Math.random() > 0.7,
        }));
        
        setProducts(productsWithExtras);
        
        // Initialize price range based on actual data
        if (productsWithExtras.length > 0) {
          const minPrice = Math.floor(Math.min(...productsWithExtras.map(p => p.price ?? 0)));
          const maxPrice = Math.ceil(Math.max(...productsWithExtras.map(p => p.price ?? 0)));
          setPriceRange([minPrice, maxPrice]);
        }

        // set category options
        const categories = new Set(productsWithExtras.map(p => p.category ?? ''));
        setCategoryOptions(Array.from(categories));
      } catch (error) {
        console.error('Error fetching products:', error);
        
        // Generate mock data for the PoC if API fails
        const mockProducts: Product[] = Array.from({ length: 30 }, (_, i) => ({
          product_id: `product-${i + 1}`,
          name: `Brazilian ${i % 7 === 0 ? 'Coffee' : i % 7 === 1 ? 'Craft' : i % 7 === 2 ? 'Food' : i % 7 === 3 ? 'Beauty' : i % 7 === 4 ? 'Fashion' : 'Home Decor'} ${i + 1}`,
          description: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed euismod, justo vel tincidunt ultricies, nunc magna ultricies nunc.',
          price: Math.floor(Math.random() * 200) + 20,
          imageUrl: `https://picsum.photos/400/300?random=${i}`,
          category: i % 7 === 0 ? 'Coffee' : i % 7 === 1 ? 'Crafts' : i % 7 === 2 ? 'Food' : i % 7 === 3 ? 'Beauty' : i % 7 === 4 ? 'Fashion' : 'Home Decor',
          rating: Math.random() * 2 + 3,
          stock: Math.floor(Math.random() * 50),
          isNew: Math.random() > 0.7,
          isOnSale: Math.random() > 0.8,
          isFavorite: Math.random() > 0.7,
        }));
        
        setProducts(mockProducts);
        setPriceRange([0, 200]);
      } finally {
        setLoading(false);
      }
    };
    
    fetchProducts();
  }, []);
  
  // Apply filters and sorting
  useEffect(() => {
    let result = [...products];
    
    // Filter by category
    if (category && category !== 'All Categories') {
      result = result.filter(product => 
        product.category?.toLowerCase() === category.toLowerCase()
      );
    }
    
    // Filter by price range
    result = result.filter(
      product => product.price && product.price >= priceRange[0] && product.price <= priceRange[1]
    );
    
    // Filter by search query
    if (searchQuery) {
      const query = searchQuery.toLowerCase();
      result = result.filter(
        product =>
          product.name?.toLowerCase().includes(query) ||
          product.description?.toLowerCase().includes(query) ||
          product.category?.toLowerCase().includes(query)
      );
    }
    
    // Filter by stock
    if (inStockOnly) {
      result = result.filter(product => product.stock && product.stock > 0);
    }
    
    // Apply sorting
    switch (sortBy) {
      case 'price_asc':
        result.sort((a, b) => (a.price ?? 0) - (b.price ?? 0));
        break;
      case 'price_desc':
        result.sort((a, b) => (b.price ?? 0) - (a.price ?? 0));
        break;
      case 'rating':
        result.sort((a, b) => (b.rating ?? 0) - (a.rating ?? 0));
        break;
      case 'newest':
      default:
        // For demo purposes, we'll just randomize for "newest"
        result.sort(() => Math.random() - 0.5);
        break;
    }
    
    // Update URL with filter params for shareability
    const params: { [key: string]: string } = {};
    if (category !== 'All Categories') params.category = category;
    if (searchQuery) params.q = searchQuery;
    if (sortBy !== 'newest') params.sort = sortBy;
    
    setSearchParams(params);
    setFilteredProducts(result);
  }, [products, category, priceRange, searchQuery, inStockOnly, sortBy, setSearchParams]);
  
  // Handle pagination
  const currentProducts = filteredProducts.slice(
    (page - 1) * productsPerPage,
    page * productsPerPage
  );
  
  const totalPages = Math.ceil(filteredProducts.length / productsPerPage);
  
  const handlePageChange = (event: React.ChangeEvent<unknown>, value: number) => {
    setPage(value);
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };
  
  const handleSortChange = (event: SelectChangeEvent) => {
    setSortBy(event.target.value);
  };
  
  const handleCategoryChange = (event: SelectChangeEvent) => {
    setCategory(event.target.value);
    setPage(1);
  };
  
  const handlePriceRangeChange = (event: Event, newValue: number | number[]) => {
    setPriceRange(newValue as number[]);
  };
  
  const handleSearchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setSearchQuery(event.target.value);
    setPage(1);
  };
  
  const handleInStockChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setInStockOnly(event.target.checked);
    setPage(1);
  };
  
  const toggleFilterDrawer = () => {
    setFilterDrawerOpen(!filterDrawerOpen);
  };
  
  const handleAddToCart = async (productId: string) => {
    await apiService.addToCart(productId);
  };
  
  const handleToggleFavorite = async (productId: string) => {
    await apiService.toggleFavorite(productId);
    
    // Update local state
    setProducts(prevProducts =>
      prevProducts.map(product =>
        product.product_id === productId
          ? { ...product, isFavorite: !product.isFavorite }
          : product
      )
    );
  };
  
  const renderFilters = () => (
    <Box sx={{ mb: { xs: 2, md: 0 } }}>
      <Typography variant="h6" gutterBottom fontWeight="bold">
        Filters
      </Typography>
      
      <Box sx={{ mb: 3 }}>
        <Typography gutterBottom>Category</Typography>
        <FormControl fullWidth size="small">
          <Select
            value={category}
            onChange={handleCategoryChange}
            displayEmpty
          >
            {categoryOptions.map(option => (
              <MenuItem key={option} value={option}>
                {option}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>
      
      <Box sx={{ mb: 3 }}>
        <Typography gutterBottom>Price Range</Typography>
        <Box sx={{ px: 1 }}>
          <Slider
            value={priceRange}
            onChange={handlePriceRangeChange}
            min={0}
            max={1000}
            valueLabelDisplay="auto"
            valueLabelFormat={(value) => `R$ ${value}`}
          />
        </Box>
        <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
          <Typography variant="body2">R$ {priceRange[0]}</Typography>
          <Typography variant="body2">R$ {priceRange[1]}</Typography>
        </Box>
      </Box>
      
      <Box sx={{ mb: 3 }}>
        <FormGroup>
          <FormControlLabel
            control={
              <Checkbox
                checked={inStockOnly}
                onChange={handleInStockChange}
                color="primary"
              />
            }
            label="In Stock Only"
          />
        </FormGroup>
      </Box>
      
      {isMobile && (
        <Button 
          variant="contained" 
          fullWidth
          onClick={toggleFilterDrawer}
          sx={{ mt: 2 }}
        >
          Apply Filters
        </Button>
      )}
    </Box>
  );
  
  return (
    <Container maxWidth="lg" sx={{ py: 4 }}>
      <Breadcrumbs sx={{ mb: 3 }}>
        <Link component={RouterLink} to="/" color="inherit">
          Home
        </Link>
        <Typography color="text.primary">Products</Typography>
        {category !== 'All Categories' && (
          <Typography color="text.primary">{category}</Typography>
        )}
      </Breadcrumbs>
      
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" component="h1" fontWeight="bold">
          {category !== 'All Categories' ? category : 'All Products'}
        </Typography>
        
        {isMobile && (
          <IconButton onClick={toggleFilterDrawer} color="primary">
            <FilterListIcon />
          </IconButton>
        )}
      </Box>
      
      <Box sx={{ display: 'flex', mb: 3 }}>
        <TextField
          variant="outlined"
          placeholder="Search products..."
          value={searchQuery}
          onChange={handleSearchChange}
          size="small"
          sx={{ flexGrow: 1, mr: 2 }}
        />
        
        <FormControl sx={{ minWidth: 200 }} size="small">
          <InputLabel id="sort-select-label">Sort By</InputLabel>
          <Select
            labelId="sort-select-label"
            value={sortBy}
            label="Sort By"
            onChange={handleSortChange}
          >
            {sortOptions.map(option => (
              <MenuItem key={option.value} value={option.value}>
                {option.label}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>
      
      <Grid container spacing={3}>
        {/* Filters for desktop */}
        {!isMobile && (
          <Grid item xs={12} md={3}>
            <Paper sx={{ p: 3 }} elevation={1}>
              {renderFilters()}
            </Paper>
          </Grid>
        )}
        
        {/* Products grid */}
        <Grid item xs={12} md={isMobile ? 12 : 9}>
          {loading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 8 }}>
              <CircularProgress />
            </Box>
          ) : (
            <>
              {filteredProducts.length === 0 ? (
                <Paper sx={{ p: 4, textAlign: 'center' }}>
                  <Typography variant="h6" gutterBottom>
                    No products found
                  </Typography>
                  <Typography variant="body1" color="text.secondary">
                    Try adjusting your filters or search query
                  </Typography>
                </Paper>
              ) : (
                <>
                  <Grid container spacing={3}>
                    {currentProducts.map(product => (
                      <Grid item xs={12} sm={6} md={4} key={product.product_id}>
                        <ProductCard
                          {...product}
                          onAddToCart={handleAddToCart}
                          onToggleFavorite={handleToggleFavorite}
                        />
                      </Grid>
                    ))}
                  </Grid>
                  
                  {totalPages > 1 && (
                    <Box sx={{ display: 'flex', justifyContent: 'center', mt: 4 }}>
                      <Pagination
                        count={totalPages}
                        page={page}
                        onChange={handlePageChange}
                        color="primary"
                        size={isMobile ? 'small' : 'medium'}
                      />
                    </Box>
                  )}
                </>
              )}
            </>
          )}
        </Grid>
      </Grid>
      
      {/* Mobile filter drawer */}
      <Drawer
        anchor="left"
        open={filterDrawerOpen}
        onClose={toggleFilterDrawer}
      >
        <Box sx={{ width: 280, p: 3 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
            <Typography variant="h6" fontWeight="bold">
              Filters
            </Typography>
            <IconButton onClick={toggleFilterDrawer} size="small">
              <CloseIcon />
            </IconButton>
          </Box>
          <Divider sx={{ mb: 3 }} />
          {renderFilters()}
        </Box>
      </Drawer>
    </Container>
  );
};

export default ProductPage; 