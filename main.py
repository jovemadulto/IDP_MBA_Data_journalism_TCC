import utils


if __name__ == "__main__":
    utils.download_files()
    df = utils.load_into_dataframe(
        filename="extrato_bancario_candidato_2020.csv", major_cities_only=False
    )

    mult_donations = (
        df.groupby(["NR_CPF_CNPJ_CONTRAPARTE"])
        .agg(
            {"NM_PRESTADOR_CONTA": lambda cand: len(set(cand)), "VR_LANCAMENTO": "sum"}
        )
        .query("NM_PRESTADOR_CONTA > 1")
        .reset_index()
    )

    mult_don_df = (
        df[
            df["NR_CPF_CNPJ_CONTRAPARTE"].isin(
                mult_donations["NR_CPF_CNPJ_CONTRAPARTE"]
            )
        ]
        .drop(
            columns=[
                "DS_CARGO_PRESTADOR_CONTA",
                "TP_PESSOA",
                "DT_LANCAMENTO",
                "NR_DOCUMENTO",
            ]
        )
        .rename(
            columns={
                "NR_CNPJ_PRESTADOR_CONTA": "CPF_CNPJ_CANDIDATO",
                "NM_PRESTADOR_CONTA": "NOME_CANDIDATO",
                "SG_PARTIDO": "SIGLA_PARTIDO",
                "VR_LANCAMENTO": "VALOR",
                "NR_CPF_CNPJ_CONTRAPARTE": "CPF_CNPJ_DOADOR",
                "NM_CONTRAPARTE": "NOME_DOADOR",
            }
        )
        .reset_index(drop=True)
    )
    mult_don_df.to_csv("doadores-para-multiplos-candidatos.csv", index=False)

    top_10_donors = (
        mult_don_df.groupby(["CPF_CNPJ_DOADOR", "NOME_DOADOR"])
        .agg({"VALOR": "sum"})
        .sort_values("VALOR", ascending=False)
        .head(10)
    )

    utils.create_gephi_graph(
        mult_don_df,
        clean=True,
        multiple_donors=top_10_donors.reset_index()["CPF_CNPJ_DOADOR"].to_list(),
    )
