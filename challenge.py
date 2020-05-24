import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import time
import psycopg2

# Define auto-ETL function
def auto_etl(wiki_data_etl, kaggle_data_etl, ratings_data_etl, table_a, table_b):
    
    # Delete existing data from PostgreSQL tables but leave tables
    try:
        db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
        engine = create_engine(db_string)
        # connect to the PostgreSQL database
        con = psycopg2.connect(db_string)
        # create a new cursor
        cur = con.cursor()
        # execute the TRUNCATE statement for table_a passed in original function
        cur.execute(f"TRUNCATE {table_a} CASCADE;")
        # get the number of TRUNCATED rows
        rows_deleted_a = cur.rowcount
        # execute the TRUNCATE statement for table_b passed in original function
        cur.execute(f"TRUNCATE {table_b} CASCADE;")
        # get the number of TRUNCATED rows
        rows_deleted_b = cur.rowcount
        # Commit the changes to the database
        con.commit()
        # Close communication with the PostgreSQL database
        cur.close()
        con.close()
        print(f"{rows_deleted_a} rows removed from {table_a} and {rows_deleted_b} rows removed from {table_b}")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    # Declare directory with data files
    file_dir = "/Volumes/Samsung_T3/Vanderbilt/Class folder/Movies-ETL/"
    
    #Load JSON file
    try:
        with open(f'{file_dir}{wiki_data_etl}', mode='r') as file:
            wiki_movies_raw = json.load(file)
    except:
        print("Error loading JSON file")
    
    # Create a dataframe from JSON data, then limit it to movies (not TV shows) with a director and IMDb link
    try:
        wiki_movies_df = pd.DataFrame(wiki_movies_raw)
        wiki_movies = [movie for movie in wiki_movies_raw
                   if ('Director' in movie or 'Directed by' in movie)
                       and 'imdb_link' in movie
                       and 'No. of episodes' not in movie]
        wiki_movies_df = pd.DataFrame(wiki_movies)
    except:
        print("Error creating dataframe from JSON data")
    
    # Define function to combine similar columns into one
    def clean_movie(movie):
        movie = dict(movie) #create a non-destructive copy
        alt_titles = {}
        
        # combine alternate titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune-Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        # merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie
    
    # Run the function for all movies and update dataframe
    try:
        clean_movies = [clean_movie(movie) for movie in wiki_movies]
        wiki_movies_df = pd.DataFrame(clean_movies)
    except:
        print("Clean movie function error")
    
    # Remove duplicates
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    except:
        print("Error removing duplicates")
    
    # Remove columns with 90+% null values
    try:
        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
        wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    except:
        print("Error removing 90% null columns")
    
    # Standardizes box office data
    try:
        box_office = wiki_movies_df['Box office'].dropna()
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
        form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
        box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        def parse_dollars(s):
            if type(s) != str:
                return np.nan
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
                s = re.sub('\$|\s|[a-zA-Z]','', s)
                value = float(s) * 10**6
                return value
            elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
                s = re.sub('\$|\s|[a-zA-Z]','', s)
                value = float(s) * 10**9
                return value
            elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
                s = re.sub('\$|,','', s)
                value = float(s)
                return value
            else:
                return np.nan
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        wiki_movies_df.drop('Box office', axis=1, inplace=True)
    except:
        print("Error standardizing box office data")
    
    # Standardizes budget data
    try:
        budget = wiki_movies_df['Budget'].dropna()
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
        matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
        budget = budget.str.replace(r'\[\d+\]\s*', '')
        wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        wiki_movies_df.drop('Budget', axis=1, inplace=True)
    except:
        print("Error standardizing budget data")
    
    # Standardizes release date data
    try:
        release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    except:
        print("Error standardizing release date data")

    # Standardizes running time data
    try:
        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        wiki_movies_df.drop('Running time', axis=1, inplace=True)
    except:
        print("Error standardizing running time data")
    
    # Read in Kaggle data and ratings data
    try:
        kaggle_metadata = pd.read_csv(kaggle_data_etl)
        ratings = pd.read_csv(ratings_data_etl)
    except:
        print("Error reading Kaggle and ratings csv data")
    
    # Remove adult movies
    try:
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    except:
        print("Error removing adult movies")
    
    # Convert columns to proper dtypes
    try:
        kaggle_metadata['video'] == 'True'
        kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    except:
        print("Error converting columns to proper dtypes")
    
    # Merge kaggle and wiki
    try:
        movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    except:
        print("Error merging Wiki and Kaggle data")
    
    # Drop merge error
    try:
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
    except:
        print("Error dropping merge anomaly - may not exist")
    
    # Competing data:
    # Wiki                     Movielens                Resolution
    #--------------------------------------------------------------------------
    # title_wiki               title_kaggle             Drop Wiki
    # release_date_wiki        release_date_kaggle      Drop Wiki
    # Language                 original_language        Drop Wiki
    # Production company(s)    production_companies     Drop Wiki
    try:
        movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    except:
        print("Error dropping Wiki columns")
    
    # Competing data:
    # Wiki                     Movielens                Resolution
    #--------------------------------------------------------------------------
    # running_time             runtime                  Keep Kaggle; fill in zeroes with Wiki
    # budget_wiki              budget_kaggle            Keep Kaggle; fill in zeroes with Wiki
    # box_office               revenue                  Keep Kaggle; fill in zeroes with Wiki
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)
    try:
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    except:
        print("Error filling in zeroes with Wiki data")
    
    # Drop video column
    try:
        movies_df.drop(columns=['video'], inplace=True)
    except:
        print("Error dropping video column")
    
    # Re-order columns
    try:
        movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                           'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                           'genres','original_language','overview','spoken_languages','Country',
                           'production_companies','production_countries','Distributor',
                           'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                          ]]
    except:
        print("Error re-ordering columns")
    
    # Rename columns
    try:
        movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'release_date_kaggle':'release_date',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'
                     }, axis='columns', inplace=True)
    except:
        print("Error renaming columns")
    
    # Get ratings from ratings data
    try:
        rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating', values='count')
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    except:
        print("Error getting ratings data")
    
    # Load movies table into PostgreSQL
    try:
        db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
        engine = create_engine(db_string)
        movies_df.to_sql(name=table_a, con=engine, if_exists='append')
    except:
        print("Error loading movies table into PostgreSQL")
    
    # Load ratings table into PostgreSQL
    rows_imported = 0
    # get the start_time from time.time()
    start_time = time.time()
    try:
        for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            data.to_sql(name=table_b, con=engine, if_exists='append')
            rows_imported += len(data)

            # add elapsed time to final print out
            print(f'Done. {time.time() - start_time} total seconds elapsed')
    except:
        print("Error loading ratings table into PostgreSQL")
        
    # Acknowledge completion
    print("Auto-ETL successfully completed!")

auto_etl("wikipedia.movies.json","movies_metadata.csv","ratings.csv","movies","ratings")