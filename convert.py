
lst2 = []
def convert(lst: list[str]):
    for i in lst:
        lst2.append(i.upper())
        
    return lst2



print(convert(["ID", "acousticness", "danceability","disc_number","duration_ms","energy","instrumentalness",
                         "key","liveness","loudness","mode","popularity","speechiness", "tempo","time_signature","valence"]))
