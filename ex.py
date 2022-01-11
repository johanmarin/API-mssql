import API.functions as f_api


def get_query():
    df = f_api.get_data('SELECT TOP(10) * FROM dbo.creCreditos;')
    return df

if __name__=="__main__":
    print(get_query())