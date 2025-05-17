-- Create external table for transformed YouTube data
CREATE EXTERNAL TABLE IF NOT EXISTS youtube_trending_data (
  video_id STRING,
  title STRING,
  publish_time DATE,
  trending_date DATE,
  channel_title STRING,
  category_id STRING,
  category_name STRING,
  tags STRING,
  views INT,
  likes INT,
  dislikes INT,
  comment_count INT,
  thumbnail_link STRING,
  comments_disabled BOOLEAN,
  ratings_disabled BOOLEAN,
  video_error_or_removed BOOLEAN,
  description STRING,
  days_to_trend INT,
  engagement_rate FLOAT,
  like_dislike_ratio FLOAT,
  popularity_level STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://cloud-project-g9/transformed-zone/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Top performing videos by engagement rate
SELECT 
  title,
  channel_title,
  category_name,
  views,
  likes,
  engagement_rate,
  trending_date
FROM 
  youtube_trending_data
ORDER BY 
  engagement_rate DESC
LIMIT 20;

-- Average engagement metrics by category
SELECT 
  category_name,
  AVG(views) AS avg_views,
  AVG(likes) AS avg_likes,
  AVG(comment_count) AS avg_comments,
  AVG(engagement_rate) AS avg_engagement
FROM 
  youtube_trending_data
GROUP BY 
  category_name
ORDER BY 
  avg_views DESC;

-- Most disliked categories
SELECT 
  category_name,
  AVG(dislikes) AS avg_dislikes,
  SUM(dislikes) AS total_dislikes
FROM 
  youtube_trending_data
GROUP BY 
  category_name
ORDER BY 
  avg_dislikes DESC;

-- Top influencers (channels)
SELECT 
  channel_title,
  COUNT(*) AS video_count,
  SUM(views) AS total_views,
  AVG(engagement_rate) AS avg_engagement,
  MAX(views) AS max_views
FROM 
  youtube_trending_data
GROUP BY 
  channel_title
HAVING 
  video_count > 5
ORDER BY 
  avg_engagement DESC
LIMIT 15;

-- Days to trend analysis
SELECT 
  category_name,
  AVG(days_to_trend) AS avg_days_to_trend
FROM 
  youtube_trending_data
GROUP BY 
  category_name
ORDER BY 
  avg_days_to_trend;

-- Popularity distribution by category
SELECT 
  category_name,
  popularity_level,
  COUNT(*) AS video_count
FROM 
  youtube_trending_data
GROUP BY 
  category_name, popularity_level
ORDER BY 
  category_name, popularity_level;
