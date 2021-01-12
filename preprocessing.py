import pandas as pd

platform = []


def preprocess_release_year(df):
    year = []
    for added in df['Date Added']:
        if "," in str(added):
            x = str(added).split(", ")
            year.append(x[1])
        else:
            year.append(added)
    df['Date Added'] = year


def fillNa1(df):
    df['Original'] = df['Original'].fillna(False)
    for column in df.columns:
        if column != 'Orignal':
            df[column] = df[column].fillna("-")


def fillNa2(df):
    for column in df.columns:
        df[column] = df[column].fillna("-")


def to_csv(df, output):
    df.to_csv(output, encoding='utf-8', index=False)


if __name__ == '__main__':
    dataset1 = pd.read_csv("datasets/netflix_imdb_combined.csv")
    dataset2 = pd.read_csv("datasets/trending_netflix_imdb_combined.csv")
    fillNa1(dataset1)
    fillNa2(dataset2)
    preprocess_release_year(dataset1)
    preprocess_release_year(dataset2)
    to_csv(dataset1, 'netflix_imdb_combined_preprocessed.csv')
    to_csv(dataset2, 'trending_netflix_imdb_combined_preprocessed.csv')

