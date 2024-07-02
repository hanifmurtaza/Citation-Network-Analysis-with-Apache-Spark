from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Citation Network Analysis") \
    .getOrCreate()

# Load the dataset
df = spark.read.parquet("C:/Users/HANIF/Downloads/dblp_v14.json/dblp_v14.parquet")

# Task 1: Top Cited Papers
top_cited_papers = df.select("id", "n_citation") \
    .orderBy(F.col("n_citation").desc()) \
    .limit(10)

# Convert to Pandas for visualization
top_cited_papers_pd = top_cited_papers.toPandas()

# Plot Top Cited Papers
plt.figure(figsize=(12, 8))
plt.barh(top_cited_papers_pd['id'], top_cited_papers_pd['n_citation'])
plt.xlabel('Citations Count')
plt.ylabel('Paper ID')
plt.title('Top 10 Cited Papers')
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

# Task 2: Top Cited Authors
exploded_authors_df = df.withColumn("author", F.explode("authors.name"))
authors_df = exploded_authors_df.groupBy("author") \
    .agg(F.sum("n_citation").alias("citation_count"))

top_cited_authors = authors_df.orderBy(F.col("citation_count").desc()).limit(10)

# Convert to Pandas for visualization
top_cited_authors_pd = top_cited_authors.toPandas()

# Plot Top Cited Authors
plt.figure(figsize=(12, 8))
plt.barh(top_cited_authors_pd['author'], top_cited_authors_pd['citation_count'])
plt.xlabel('Citations Count')
plt.ylabel('Author Name')
plt.title('Top 10 Cited Authors')
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

# Task 3: Collaboration Patterns
co_author_pairs_df = exploded_authors_df.alias("df1") \
    .join(exploded_authors_df.alias("df2"), F.col("df1.id") == F.col("df2.id")) \
    .filter(F.col("df1.author") < F.col("df2.author")) \
    .select(F.col("df1.author").alias("author1"), F.col("df2.author").alias("author2"))

collaboration_counts_df = co_author_pairs_df.groupBy("author1", "author2").count()

top_collaborations = collaboration_counts_df.orderBy(F.col("count").desc()).limit(10)

# Convert to Pandas for visualization
top_collaborations_pd = top_collaborations.toPandas()

# Plot Top Collaborations
plt.figure(figsize=(12, 8))
plt.barh(top_collaborations_pd.apply(lambda row: f'{row["author1"]} & {row["author2"]}', axis=1), top_collaborations_pd['count'])
plt.xlabel('Number of Collaborations')
plt.ylabel('Co-Author Pairs')
plt.title('Top 10 Co-Author Collaborations')
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

# Task 4: Publication Trends by Year
pub_year_df = df.groupBy("year").count().alias("publication_count")
pub_year_df = pub_year_df.filter(pub_year_df.year.isNotNull())  # Remove records with null year
pub_year_df = pub_year_df.orderBy("year")

# Convert to Pandas for visualization
pub_year_pd = pub_year_df.toPandas()

# Plot Publication Trends by Year
plt.figure(figsize=(12, 8))
plt.plot(pub_year_pd['year'], pub_year_pd['count'])
plt.xlabel('Published Year')
plt.ylabel('Number of Publications')
plt.title('Publication Trends by Year')
plt.tight_layout()
plt.show()

# Task 5: Top Venues by Publications
top_venues_df = df.groupBy("venue.raw").count().orderBy(F.col("count").desc()).limit(10)

# Convert to Pandas for visualization
top_venues_pd = top_venues_df.toPandas()

# Plot Top Venues by Publications
plt.figure(figsize=(12, 8))
plt.barh(top_venues_pd['raw'], top_venues_pd['count'])
plt.xlabel('Number of Publications')
plt.ylabel('Venue')
plt.title('Top 10 Venues by Publications')
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

print("Data processing and analysis completed.")
spark.stop()
