import pandas as pd
import os

DATA_PATH = "../data/processed/"
RESULTS_PATH = "../results/"

if not os.path.exists(RESULTS_PATH):
    os.makedirs(RESULTS_PATH)

def load_data():
    file_list = {
        'songs': 'songs.csv',
        'genres': 'genres.csv',
        'song_genres': 'song_genres.csv',
        'release_info': 'release_info.csv',
        'audio_features': 'audio_features.csv',
        'similar_songs': 'similar_songs.csv',
        'activity': 'activity.csv'
    }
    
    data = {}
    for key, filename in file_list.items():
        path = os.path.join(DATA_PATH, filename)
        try:
            data[key] = pd.read_csv(
                path, 
                on_bad_lines='skip',  
                quotechar='"',        
                skipinitialspace=True, 
                low_memory=False
            )
        except Exception as e:
            print(f"Error: {filename}: {e}")
    return data

def process_questions(data):
    rock_songs = (
        data['songs']
        .merge(data['song_genres'], on='song_id')
        .merge(data['genres'], on='genre_id')
        .merge(data['release_info'], on='song_id')
    )
    q1_result = rock_songs[rock_songs['genre_name'].str.lower() == 'rock'] \
                .sort_values(by='Popularity', ascending=False).head(10)
    q1_result[['song', 'Artist(s)', 'Popularity']].to_csv(os.path.join(RESULTS_PATH, 'q1_top_rock_songs.csv'), index=False)

    data['release_info']['Year'] = pd.to_datetime(data['release_info']['Release Date']).dt.year
    positiveness_trends = (
        data['release_info']
        .merge(data['audio_features'], on='song_id')
    )

    q2_result = positiveness_trends[positiveness_trends['Year'] >= 2010] \
                .groupby('Year')['Positiveness'].mean().reset_index()
    q2_result.to_csv(os.path.join(RESULTS_PATH, 'q2_positiveness_trends.csv'), index=False)

    q3_result = (
        data['similar_songs']
        .sort_values(by=['song_id', 'similarity_score'], ascending=[True, False])
        .groupby('song_id').head(3)
    )
    q3_result.to_csv(os.path.join(RESULTS_PATH, 'q3_top_3_similar_songs.csv'), index=False)

    artist_popularity = (
        data['songs']
        .merge(data['song_genres'], on='song_id')
        .merge(data['genres'], on='genre_id')
        .merge(data['release_info'], on='song_id')
        .groupby(['genre_name', 'Artist(s)'])['Popularity'].mean().reset_index()
    )

    artist_popularity['Rank'] = artist_popularity.groupby('genre_name')['Popularity'] \
                                .rank(ascending=False, method='dense')
    q4_result = artist_popularity.sort_values(['genre_name', 'Rank'])
    q4_result.to_csv(os.path.join(RESULTS_PATH, 'q4_artist_genre_rank.csv'), index=False)

    def time_to_seconds(t_str):
        try:
            m, s = map(int, str(t_str).split(':'))
            return m * 60 + s
        except: return 0

    duration_data = (
        data['songs']
        .merge(data['song_genres'], on='song_id')
        .merge(data['genres'], on='genre_id')
        .merge(data['release_info'], on='song_id')
    )
    duration_data['Length_Sec'] = duration_data['Length'].apply(time_to_seconds)
    q5_result = duration_data.groupby('genre_name')['Length_Sec'].mean().reset_index()
    q5_result['Avg_Length_Min'] = (q5_result['Length_Sec'] / 60).round(2)
    q5_result.to_csv(os.path.join(RESULTS_PATH, 'q5_genre_duration_analysis.csv'), index=False)

    road_trip_candidates = (
        data['songs']
        .merge(data['release_info'], on='song_id')
        .merge(data['audio_features'], on='song_id')
        .merge(data['activity'], on='song_id')
    )
    
    q6_result = road_trip_candidates[
        (road_trip_candidates['Length'] > '03:00') & 
        (road_trip_candidates['Tempo'] >= 100) & 
        (road_trip_candidates['Tempo'] <= 120) & 
        (road_trip_candidates['Good for Driving'] == 1)
    ]
    q6_result[['song', 'Artist(s)', 'Tempo', 'Length']].to_csv(os.path.join(RESULTS_PATH, 'q6_road_trip_playlist.csv'), index=False)

if __name__ == "__main__":
    try:
        datasets = load_data()
        process_questions(datasets)
    except Exception as e:
        print(f"Error: {e}")