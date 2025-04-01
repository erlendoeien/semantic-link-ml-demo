import axios from 'axios';

// Create axios instance with base URL
const api = axios.create({
  baseURL: process.env.REACT_APP_API_URL || 'http://127.0.0.1:5000/api',
  headers: {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Credentials': 'true'
  },
  withCredentials: true,
});

// Cache implementation
interface Cache<T> {
  data: T;
  timestamp: number;
  expiry: number; // expiry time in milliseconds
}

const cache: Record<string, Cache<any>> = {};

function getFromCache<T>(key: string, expiryTime: number = 5 * 60 * 1000): T | null {
  const cachedItem = cache[key];
  if (cachedItem && Date.now() - cachedItem.timestamp < cachedItem.expiry) {
    console.log(`Cache hit for ${key}`);
    return cachedItem.data;
  }
  console.log(`Cache miss for ${key}`);
  return null;
}

function setCache<T>(key: string, data: T, expiryTime: number = 5 * 60 * 1000): void {
  cache[key] = {
    data,
    timestamp: Date.now(),
    expiry: expiryTime
  };
}

interface IProductRaw {
  product_id: string;
  Category?: string;
  product_segment?: string;
  product_category_name?: string;
  product_description_lenght?: number;
  product_height_cm?: number;
  product_length_cm?: number;
  product_name_lenght?: number;
  product_photos_qty?: number;
  product_weight_g?: number;
  product_width_cm?: number;
}

interface IProductRecommendation {
  products: {
    product_id: string;
    Category: string;
    Product_Segment: string;
  }
}

export interface Product extends IProductRaw {
  imageUrl?: string;
  name?: string;
  description?: string;
  price?: number;
  category?: string;
  rating?: number;
  stock?: number;
  isNew?: boolean;
  isOnSale?: boolean;
  isFavorite?: boolean;
  details?: string;
}

export interface Category {
  original_product_category: string;
  higher_order_category: string;
}

export interface User {
  id: string;
  name: string;
  email: string;
}

function enrich_products(products: IProductRaw[]): Product[] {
  return products.map((product, i) =>   enrich_product(product, i));
}

function enrich_product(product: IProductRaw, i?: number): Product {
  if (i === undefined) {
    i = Math.floor(Math.random() * 1000);
  }
  return {
    ...product,
    name: `Brazilian ${product.Category || 'Unknown'} ${product.product_id}`,
    description: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed euismod, justo vel tincidunt ultricies, nunc magna ultricies nunc.',
    price: Math.floor(Math.random() * 200) + 20,
    imageUrl: `https://picsum.photos/400/300?random=${i}`,
    category: product.Category || 'Unknown',
    rating: Math.random() * 2 + 3,
    stock: Math.floor(Math.random() * 50),
  };
}

// API functions
export const apiService = {
  // Products
  getProducts: async (): Promise<Product[]> => {
    console.log("getProducts");
    const cacheKey = 'products';
    const cachedData = getFromCache<Product[]>(cacheKey);
    
    if (cachedData) {
      return cachedData;
    }
    
    const response = await api.get('/products');
    const enrichedProducts = enrich_products(response.data);
    setCache(cacheKey, enrichedProducts);
    return enrichedProducts;
  },
  
  getProductById: async (id: string): Promise<Product> => {
    console.log("getProductById");
    const cacheKey = `product_${id}`;
    const cachedData = getFromCache<Product>(cacheKey);
    
    if (cachedData) {
      return cachedData;
    }
    
    const response = await api.get(`/products/${id}`);
    const enrichedProduct = enrich_product(response.data);
    setCache(cacheKey, enrichedProduct);
    return enrichedProduct;
  },
  
  // Recommendations
  getSimilarProducts: async (productId: string): Promise<Product[]> => {
    const cacheKey = `similar_products_${productId}`;
    const cachedData = getFromCache<Product[]>(cacheKey);
    
    if (cachedData) {
      return cachedData;
    }
    
    const response = await api.get(`/recommendations/similar/${productId}`);
    const enrichedProducts = enrich_products(response.data);
    setCache(cacheKey, enrichedProducts);
    return enrichedProducts;
  },
  
  getPersonalRecommendations: async (userId: string): Promise<Product[]> => {
    console.log("getPersonalRecommendations");
    const cacheKey = `personal_recommendations_${userId}`;
    const cachedData = getFromCache<Product[]>(cacheKey);
    
    if (cachedData) {
      return cachedData;
    }
    
    const response = await api.get<IProductRecommendation[]>(`/recommendations/personal/${userId}`);
    // Transform the GraphQL response into Product array

    const recommendations = response.data.map(item => ({
      product_id: item.products.product_id,
      Category: item.products.Category,
      product_segment: item.products.Product_Segment,
      // Add other required fields with default values
      name: `Brazilian ${item.products.Category || 'Unknown'} ${item.products.product_id}`,
      description: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed euismod, justo vel tincidunt ultricies, nunc magna ultricies nunc.',
      price: Math.floor(Math.random() * 200) + 20,
      imageUrl: `https://picsum.photos/400/300?random=${item.products.product_id}`,
      category: item.products.Category || 'Unknown',
      rating: Math.random() * 2 + 3,
      stock: Math.floor(Math.random() * 50),
    }));
    setCache(cacheKey, recommendations);
    return recommendations;
  },
  
  // Mock functions for demo/PoC (would be implemented on the backend in a real app)
  addToCart: async (productId: string, quantity: number = 1): Promise<void> => {
    // Mock implementation - in a real app, this would call the backend
    console.log(`Added product ${productId} to cart, quantity: ${quantity}`);
    return Promise.resolve();
  },
  
  toggleFavorite: async (productId: string): Promise<void> => {
    // Mock implementation - in a real app, this would call the backend
    console.log(`Toggled favorite status for product ${productId}`);
    return Promise.resolve();
  },

  getCategories: async (): Promise<Category[]> => {
    const cacheKey = 'categories';
    const cachedData = getFromCache<Category[]>(cacheKey);
    
    if (cachedData) {
      return cachedData;
    }
    
    const response = await api.get('/categories');
    setCache(cacheKey, response.data);
    return response.data;
  },
  
  // Method to clear cache
  clearCache: (): void => {
    Object.keys(cache).forEach(key => delete cache[key]);
    console.log('Cache cleared');
  },
  
  // Method to clear specific cache entry
  clearCacheItem: (key: string): void => {
    if (cache[key]) {
      delete cache[key];
      console.log(`Cache cleared for ${key}`);
    }
  }
};

export default apiService; 