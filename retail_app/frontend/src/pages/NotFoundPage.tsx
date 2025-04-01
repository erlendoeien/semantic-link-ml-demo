import React from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { Container, Typography, Button, Box, Paper } from '@mui/material';
import ShoppingBagIcon from '@mui/icons-material/ShoppingBag';

const NotFoundPage: React.FC = () => {
  return (
    <Container maxWidth="md" sx={{ py: 8 }}>
      <Paper 
        elevation={1} 
        sx={{ 
          p: 6, 
          textAlign: 'center', 
          borderRadius: 2,
          backgroundColor: 'background.paper',
        }}
      >
        <Typography 
          variant="h1" 
          color="primary" 
          sx={{ 
            fontWeight: 700, 
            fontSize: { xs: '4rem', md: '6rem' },
            mb: 2,
          }}
        >
          404
        </Typography>
        
        <Typography 
          variant="h4" 
          gutterBottom 
          sx={{ 
            fontWeight: 600,
            mb: 2,
          }}
        >
          Page Not Found
        </Typography>
        
        <Typography 
          variant="body1" 
          color="text.secondary" 
          paragraph
          sx={{ mb: 4, maxWidth: 500, mx: 'auto' }}
        >
          The page you are looking for might have been removed, had its name changed, or is temporarily unavailable.
        </Typography>
        
        <Box 
          sx={{
            display: 'flex',
            flexDirection: { xs: 'column', sm: 'row' },
            justifyContent: 'center',
            gap: 2,
          }}
        >
          <Button
            component={RouterLink}
            to="/"
            variant="contained"
            color="primary"
            size="large"
            sx={{ px: 4 }}
          >
            Back to Home
          </Button>
          
          <Button
            component={RouterLink}
            to="/products"
            variant="outlined"
            color="primary"
            size="large"
            startIcon={<ShoppingBagIcon />}
            sx={{ px: 4 }}
          >
            Continue Shopping
          </Button>
        </Box>
      </Paper>
    </Container>
  );
};

export default NotFoundPage; 