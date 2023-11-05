import os

import pandas as pd


def read_input_file(input_file):
    """
    Reads the input file and returns a pandas dataframe.
    """
    df = pd.read_csv(input_file, delimiter=";")
    df = df.apply(
        lambda x: x.str.strip().replace(r"\s+", " ", regex=True)
        if x.dtype == "object"
        else x
    )
    df["CEP"] = df["CEP"].astype(str)
    df["CEP"] = df["CEP"].str.strip()
    return df


def validate_invoices(df):
    """
    Validates the invoices in the dataframe and returns a filtered dataframe.
    """
    df = df[
        df["CEP"].str.len().between(7, 8)
        & df["CEP"].str.isnumeric()
        & (~df["CEP"].str.match(r"^0+$"))
    ]

    return df


def create_endereco_completo(df):
    """
    Creates the "EnderecoCompleto" column to the dataframe and returns it.
    """
    df["EnderecoCompleto"] = (
        df["RuaComComplemento"]
        + ", "
        + df["Bairro"]
        + ", "
        + df["Cidade"]
        + ", "
        + df["Estado"]
        + ", "
        + df["CEP"]
    )
    return df


def separate_invoices(df):
    """
    Separates the invoices in the dataframe and returns a dictionary of dataframes.
    """
    conditions = {
        "zero": df["ValorFatura"] == 0,
        "ate_6": (df["NumeroPaginas"] <= 6) & (df["ValorFatura"] != 0),
        "ate_12": (df["NumeroPaginas"] > 6)
        & (df["NumeroPaginas"] <= 12)
        & (df["ValorFatura"] != 0),
        "mais_12": (df["NumeroPaginas"] > 12) & (df["ValorFatura"] != 0),
    }
    return {key: df[condition] for key, condition in conditions.items()}


def write_output_files(dfs, output_dir):
    """
    Writes the output files to the specified output directory.
    """
    header = ["NomeCliente", "EnderecoCompleto", "ValorFatura", "NumeroPaginas"]
    for key, df in dfs.items():
        df[header].to_csv(
            os.path.join(output_dir, f"faturas_{key}.csv"), sep=";", index=False
        )


def main(input_file, output_dir):
    """
    Main function that reads the input file, validates the invoices, separates them based on the number of pages,
    and writes the output files to the specified output directory.
    """
    df = read_input_file(input_file)
    df = validate_invoices(df)
    df = create_endereco_completo(df)
    dfs = separate_invoices(df)
    write_output_files(dfs, output_dir)


if __name__ == "__main__":
    input_file = os.path.join("input", "base_hi.txt")
    output_dir = "output"
    main(input_file, output_dir)
