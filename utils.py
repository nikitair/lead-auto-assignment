def prepare_postalcode(postalcode: str):
    res = ''
    if len(postalcode) > 0:
        if len(postalcode) == 6:
            res = f"{postalcode[0:3]} {postalcode[3:6]}"
        else:
            res = postalcode
    return res


if __name__ == "__main__":
    z1 = "N1S0C2"
    z2 = "N1S 0C2"
    z3 = "N1S H0C2"
    z4 = "N1SH0C2"
    z5 = ""

    print(prepare_postalcode(z1))
    print(prepare_postalcode(z2))
    print(prepare_postalcode(z3))
    print(prepare_postalcode(z4))
    print(prepare_postalcode(z5))