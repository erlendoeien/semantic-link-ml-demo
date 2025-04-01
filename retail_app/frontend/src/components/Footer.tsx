import React from 'react';
import { Link as RouterLink } from 'react-router-dom';
import {
  Box,
  Container,
  Grid,
  Typography,
  Link,
  Divider,
  IconButton,
  Stack,
} from '@mui/material';
import FacebookIcon from '@mui/icons-material/Facebook';
import TwitterIcon from '@mui/icons-material/Twitter';
import InstagramIcon from '@mui/icons-material/Instagram';
import YouTubeIcon from '@mui/icons-material/YouTube';

const Footer: React.FC = () => {
  return (
    <Box
      component="footer"
      sx={{
        py: 5,
        px: 2,
        mt: 'auto',
        backgroundColor: (theme) => theme.palette.grey[100],
      }}
    >
      <Container maxWidth="lg">
        <Grid container spacing={4} justifyContent="space-between">
          <Grid item xs={12} sm={6} md={3}>
            <Typography variant="h6" color="primary" gutterBottom sx={{ fontWeight: 'bold' }}>
              Brazilify
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph sx={{ maxWidth: 270 }}>
              Your one-stop destination for authentic Brazilian products and more.
            </Typography>
            <Stack direction="row" spacing={1}>
              <IconButton
                size="small"
                color="primary"
                aria-label="facebook"
                component="a"
                href="https://facebook.com"
                target="_blank"
                rel="noopener noreferrer"
              >
                <FacebookIcon />
              </IconButton>
              <IconButton
                size="small"
                color="primary"
                aria-label="twitter"
                component="a"
                href="https://twitter.com"
                target="_blank"
                rel="noopener noreferrer"
              >
                <TwitterIcon />
              </IconButton>
              <IconButton
                size="small"
                color="primary"
                aria-label="instagram"
                component="a"
                href="https://instagram.com"
                target="_blank"
                rel="noopener noreferrer"
              >
                <InstagramIcon />
              </IconButton>
              <IconButton
                size="small"
                color="primary"
                aria-label="youtube"
                component="a"
                href="https://youtube.com"
                target="_blank"
                rel="noopener noreferrer"
              >
                <YouTubeIcon />
              </IconButton>
            </Stack>
          </Grid>
          
          <Grid item xs={6} sm={3} md={2}>
            <Typography variant="subtitle1" color="text.primary" gutterBottom sx={{ fontWeight: 'bold' }}>
              Shop
            </Typography>
            <Link component={RouterLink} to="/products" color="inherit" display="block" sx={{ mb: 1 }}>
              All Products
            </Link>
            <Link component={RouterLink} to="/products?category=deals" color="inherit" display="block" sx={{ mb: 1 }}>
              Deals
            </Link>
            <Link component={RouterLink} to="/products?category=new" color="inherit" display="block" sx={{ mb: 1 }}>
              New Arrivals
            </Link>
            <Link component={RouterLink} to="/products?category=popular" color="inherit" display="block" sx={{ mb: 1 }}>
              Popular Items
            </Link>
          </Grid>
          
          <Grid item xs={6} sm={3} md={2}>
            <Typography variant="subtitle1" color="text.primary" gutterBottom sx={{ fontWeight: 'bold' }}>
              Support
            </Typography>
            <Link component={RouterLink} to="/help" color="inherit" display="block" sx={{ mb: 1 }}>
              Help Center
            </Link>
            <Link component={RouterLink} to="/contact" color="inherit" display="block" sx={{ mb: 1 }}>
              Contact Us
            </Link>
            <Link component={RouterLink} to="/shipping" color="inherit" display="block" sx={{ mb: 1 }}>
              Shipping Info
            </Link>
            <Link component={RouterLink} to="/returns" color="inherit" display="block" sx={{ mb: 1 }}>
              Returns & Exchanges
            </Link>
          </Grid>
          
          <Grid item xs={6} sm={3} md={2}>
            <Typography variant="subtitle1" color="text.primary" gutterBottom sx={{ fontWeight: 'bold' }}>
              Company
            </Typography>
            <Link component={RouterLink} to="/about" color="inherit" display="block" sx={{ mb: 1 }}>
              About Us
            </Link>
            <Link component={RouterLink} to="/careers" color="inherit" display="block" sx={{ mb: 1 }}>
              Careers
            </Link>
            <Link component={RouterLink} to="/privacy" color="inherit" display="block" sx={{ mb: 1 }}>
              Privacy Policy
            </Link>
            <Link component={RouterLink} to="/terms" color="inherit" display="block" sx={{ mb: 1 }}>
              Terms of Service
            </Link>
          </Grid>
        </Grid>
        
        <Divider sx={{ my: 3 }} />
        
        <Box sx={{ display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap' }}>
          <Typography variant="body2" color="text.secondary">
            &copy; {new Date().getFullYear()} Brazilify. All rights reserved.
          </Typography>
          <Box>
            <Link href="#" color="inherit" sx={{ mr: 2 }}>
              Privacy
            </Link>
            <Link href="#" color="inherit" sx={{ mr: 2 }}>
              Terms
            </Link>
            <Link href="#" color="inherit">
              Sitemap
            </Link>
          </Box>
        </Box>
      </Container>
    </Box>
  );
};

export default Footer; 