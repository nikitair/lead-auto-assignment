def prepare_zipcode(zipcode: str):
    res = ''
    if len(zipcode) > 0:
        if len(zipcode) == 6:
            res = f"{zipcode[0:3]} {zipcode[3:6]}"
        else:
            res = zipcode
    return res


if __name__ == "__main__":
    z1 = "N1S0C2"
    z2 = "N1S 0C2"
    z3 = "N1S H0C2"
    z4 = "N1SH0C2"
    z5 = ""

    print(prepare_zipcode(z1))
    print(prepare_zipcode(z2))
    print(prepare_zipcode(z3))
    print(prepare_zipcode(z4))
    print(prepare_zipcode(z5))