
def make_unique_name(base, dictionary):
    suffix=0
    while f"{base}{suffix}" in dictionary:
        suffix+=1
    assert f"{base}{suffix}" not in dictionary

    return f"{base}{suffix}"
