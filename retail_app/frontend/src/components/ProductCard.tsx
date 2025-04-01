import React from 'react';
import { Link as RouterLink } from 'react-router-dom';
import {
  Card,
  CardContent,
  CardMedia,
  Typography,
  Button,
  Rating,
  Box,
  Chip,
  CardActionArea,
  CardActions,
} from '@mui/material';
import ShoppingCartIcon from '@mui/icons-material/ShoppingCart';
import FavoriteIcon from '@mui/icons-material/Favorite';
import FavoriteBorderIcon from '@mui/icons-material/FavoriteBorder';

interface ProductCardProps {
  product_id: string;
  name?: string;
  price?: number;
  description?: string;
  imageUrl?: string;
  rating?: number;
  category?: string;
  isFavorite?: boolean;
  isNew?: boolean;
  isOnSale?: boolean;
  stock?: number;
  onAddToCart?: (id: string) => void;
  onToggleFavorite?: (id: string) => void;
}

const ProductCard: React.FC<ProductCardProps> = ({
  product_id,
  price,
  description,
  name,
  imageUrl,
  rating,
  category,
  isFavorite = false,
  isNew = false,
  isOnSale = false,
  stock = 0,
  onAddToCart,
  onToggleFavorite,
}) => {
  const handleAddToCart = (event: React.MouseEvent) => {
    event.preventDefault();
    event.stopPropagation();
    if (onAddToCart) {
      onAddToCart(product_id);
    }
  };

  const handleToggleFavorite = (event: React.MouseEvent) => {
    event.preventDefault();
    event.stopPropagation();
    if (onToggleFavorite) {
      onToggleFavorite(product_id);
    }
  };

  return (
    <Card
      elevation={1}
      sx={{
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        position: 'relative',
        transition: 'transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out',
        '&:hover': {
          transform: 'translateY(-5px)',
          boxShadow: '0 6px 20px rgba(0, 0, 0, 0.1)',
        },
      }}
    >
      {(isNew || isOnSale) && (
        <Box
          sx={{
            position: 'absolute',
            top: 12,
            left: 12,
            zIndex: 1,
          }}
        >
          {isNew && (
            <Chip
              label="NEW"
              color="primary"
              size="small"
              sx={{ mb: 1, fontWeight: 'bold' }}
            />
          )}
          {isOnSale && (
            <Chip
              label="SALE"
              color="error"
              size="small"
              sx={{ fontWeight: 'bold' }}
            />
          )}
        </Box>
      )}

      <Box
        sx={{
          position: 'absolute',
          top: 12,
          right: 12,
          zIndex: 1,
        }}
      >
        <Button
          onClick={handleToggleFavorite}
          color="primary"
          sx={{
            minWidth: 'auto',
            p: 1,
            borderRadius: '50%',
            backgroundColor: 'rgba(255, 255, 255, 0.8)',
            '&:hover': {
              backgroundColor: 'rgba(255, 255, 255, 0.9)',
            },
          }}
        >
          {isFavorite ? (
            <FavoriteIcon color="error" fontSize="small" />
          ) : (
            <FavoriteBorderIcon fontSize="small" />
          )}
        </Button>
      </Box>

      <CardActionArea component={RouterLink} to={`/products/${product_id}`}>
        <CardMedia
          component="img"
          height="200"
          image={imageUrl}
          alt={name}
          sx={{ objectFit: 'contain', p: 2, bgcolor: '#f5f5f5' }}
        />
        <CardContent sx={{ flexGrow: 1, pb: 1 }}>
          <Typography variant="body2" color="text.secondary" gutterBottom>
            {category}
          </Typography>
          <Typography
            variant="h6"
            component="h2"
            gutterBottom
            sx={{
              fontWeight: 'medium',
              fontSize: '1rem',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              display: '-webkit-box',
              WebkitLineClamp: 2,
              WebkitBoxOrient: 'vertical',
              lineHeight: 1.2,
              height: '2.4em',
            }}
          >
            {name}
          </Typography>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
            <Rating value={rating} precision={0.5} size="small" readOnly />
            <Typography variant="body2" color="text.secondary" sx={{ ml: 0.5 }}>
              ({rating?.toFixed(1)})
            </Typography>
          </Box>
          <Typography
            variant="body2"
            color="text.secondary"
            sx={{
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              display: '-webkit-box',
              WebkitLineClamp: 2,
              WebkitBoxOrient: 'vertical',
              mb: 2,
              height: '2.6em',
            }}
          >
            {description}
          </Typography>
          <Typography
            variant="h6"
            color="primary"
            sx={{ fontWeight: 'bold', mt: 'auto' }}
          >
            R$ {price?.toFixed(2)}
          </Typography>
          {stock > 0 ? (
            <Typography variant="body2" color="success.main">
              In Stock
            </Typography>
          ) : (
            <Typography variant="body2" color="error">
              Out of Stock
            </Typography>
          )}
        </CardContent>
      </CardActionArea>
      <CardActions sx={{ p: 2, pt: 0 }}>
        <Button
          variant="contained"
          color="primary"
          fullWidth
          startIcon={<ShoppingCartIcon />}
          onClick={handleAddToCart}
          disabled={stock === 0}
        >
          Add to Cart
        </Button>
      </CardActions>
    </Card>
  );
};

export default ProductCard; 