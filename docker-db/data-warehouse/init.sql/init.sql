
-- Table for amazon_data
CREATE TABLE public.AmazonData (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    main_category VARCHAR(255),
    sub_category VARCHAR(255),
    image TEXT,
    link TEXT,
    ratings DECIMAL(3, 1),
    no_of_ratings INTEGER,
    discount_price DECIMAL(10, 2),
    actual_price DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for product_data
CREATE TABLE public.ProductData (
    id SERIAL PRIMARY KEY,                     
    max_price NUMERIC(10, 2),                  
    min_price NUMERIC(10, 2),                  
    availability VARCHAR(100),                 
    condition VARCHAR(50),                     
    merchant VARCHAR(255),                     
    date_seen DATE,                            
    is_sale BOOLEAN,                           
    shipping VARCHAR(100),                     
    brand VARCHAR(100),                        
    sub_categories VARCHAR(255),               
    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
    date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    main_categories VARCHAR(255)        
);

-- Table for mydramalist_data
CREATE TABLE public.Mydramalist_data (
    id SERIAL PRIMARY KEY,                     
    reviewer VARCHAR(255),                     
    profile_link TEXT,                         
    review_date DATE,                          
    helpful_count INT DEFAULT 0,               
    overall_rating INT,                        
    story_rating INT,                          
    acting_rating INT,                         
    music_rating INT,                          
    rewatch_value INT,                         
    review_body TEXT,                          
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);
