import zipfile
import requests
from pathlib import Path
import pandas as pd


def download_files(fresh_files: bool = False, folder_name: str = "data") -> None:

    def extract_files(file: str) -> None:

        with zipfile.ZipFile(file, mode="r") as zpf:
            zpf.extractall(folder_name)

    def download_file_as_stream(filename, url):
        """
        Adaptado de https://stackoverflow.com/a/16696317
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Connection": "keep-alive",
        }
        with requests.get(url, headers=headers, stream=True) as r:
            with open(file_destination, "wb") as f:
                for chunk in r.iter_content(chunk_size=8_192 * 1_000):
                    f.write(chunk)

    files = {
        # # Os arquivos são retirados da página mantida pela Receita Federal do Brasil
        # # https://dadosabertos.rfb.gov.br/CNPJ/
        #
        # # A base de Sócios da Receita Federal tem as informações a respeito 
        # # das pessoas físicas que estão cadastradas como sócias de empresas.
        "Socios0.zip": "https://dadosabertos.rfb.gov.br/CNPJ/Socios0.zip",
        "Socios1.zip": "https://dadosabertos.rfb.gov.br/CNPJ/Socios1.zip",
        "Socios2.zip": "https://dadosabertos.rfb.gov.br/CNPJ/Socios2.zip",
        "Socios3.zip": "https://dadosabertos.rfb.gov.br/CNPJ/Socios3.zip",
        "Socios4.zip": "https://dadosabertos.rfb.gov.br/CNPJ/Socios4.zip",
        "Socios5.zip": "https://dadosabertos.rfb.gov.br/CNPJ/Socios5.zip",
        "Socios6.zip": "https://dadosabertos.rfb.gov.br/CNPJ/Socios6.zip",
        "Socios7.zip": "https://dadosabertos.rfb.gov.br/CNPJ/Socios7.zip",
        "Socios8.zip": "https://dadosabertos.rfb.gov.br/CNPJ/Socios8.zip",
        "Socios9.zip": "https://dadosabertos.rfb.gov.br/CNPJ/Socios9.zip",

        # # As bases de dados de prestação de contas estão disponíveis
        # # no endereço https://dadosabertos.tse.jus.br/dataset/prestacao-de-contas-eleitorais-2020
        #
        # "extrato_bancario_partido_2020.zip": "https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas_anual_partidaria/extrato_bancario_partido_2020.zip",
        "extrato_bancario_candidato_2020.zip": "https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas_anual_candidato/extrato_bancario_candidato_2020.zip",
    }

    download_folder = Path(folder_name)
    if not download_folder.exists():
        download_folder.mkdir(parents=True)

    for filename, url in files.items():
        file_destination = Path(download_folder / filename)
        if not (file_destination.exists()) or (fresh_files):
            """
            Caso o arquivo não tenha sido baixado ainda,
            ou o usuário deseje atualizar os arquivos
            """
            print(f"[+] Fazendo download de {filename}.")
            download_file_as_stream(filename, url)
            extract_files(file_destination)
            print("\n")
        else:
            print(
                f"\n[-] {filename} encontrado na pasta de downloads.\nIgnorando este arquivo..."
            )


def normalize_str(string: str) -> str:
    """
    Função auxiliar para remover os acentos de nomes de prefeitos.
    """
    import unicodedata

    try:
        normalized = unicodedata.normalize("NFKD", string)
        string = "".join([c for c in normalized if not unicodedata.combining(c)])
        string = string.casefold()
        return string
    except TypeError:
        return "error"


def load_into_dataframe(
    filetype: str,
    folder: str = "data",
    major_cities_only: bool = False,
    **kwargs
) -> pd.DataFrame:
    valid_map = {
        # Tipo de arquivo: Extensão do arquivo segundo os dados da Receita
        "socios": "SOCIOCSV",
        "extrato": "csv",
    }
    if filetype not in valid_map.keys():
        valid_types = valid_map.keys()
        valid_types = ", ".join(valid_types)
        raise ValueError(
            f"Tipo de arquivo não é válido. São aceitos somente: {valid_types}"
        )
    
    else:
        file_ext = valid_map.get(filetype)
        files_loaded = []
        if filetype == "extrato":
            for file in Path(folder).glob(f"*.{file_ext}"):
                t_df = pd.read_csv(
                    file,
                    sep=";",
                    encoding="latin-1",
                    dtype={
                        "NR_CPF_CNPJ_CONTRAPARTE": str,
                        "NM_CONTRAPARTE": str,
                    },
                    usecols=[
                        "NR_CNPJ_PRESTADOR_CONTA",
                        "DS_CARGO_PRESTADOR_CONTA",
                        "SG_PARTIDO",
                        "NM_PRESTADOR_CONTA",
                        "TP_PESSOA",
                        "DT_LANCAMENTO",
                        "NR_DOCUMENTO",
                        "VR_LANCAMENTO",
                        "NR_CPF_CNPJ_CONTRAPARTE",
                        "NM_CONTRAPARTE",
                    ],
                    decimal=",",
            )
                files_loaded.append(t_df)

            df = pd.concat(files_loaded)
            df = df[
                # Somente pessoa física
                (df["TP_PESSOA"] == 1)
                # Validação do ID pelo tamanho.
                # Somente len  < 14 pode ser considerado CPF
                & (df["NR_CPF_CNPJ_CONTRAPARTE"].apply(len) < 14)
                # Vários CPFs de doardores foram inseridos de maneira incorreta,
                # apresentando, por exemplo, todos os dígitos "1", "9" ou até mesmo "-1".
                # É necessário excluir esses resultados.
                & (df["NR_CPF_CNPJ_CONTRAPARTE"].apply(set).apply(len) > 2)
                # Considerando somente prestação de contas de prefeitos.
                # Exclui vereadores
                & (df["DS_CARGO_PRESTADOR_CONTA"] == "PREFEITO")
            ]
            df["NM_PRESTADOR_CONTA"] = df["NM_PRESTADOR_CONTA"].apply(normalize_str)
            df["NM_CONTRAPARTE"] = df["NM_CONTRAPARTE"].apply(normalize_str)

            if major_cities_only:
                mayors = (
                    normalize_str("Edvaldo Nogueira Filho"),  # Aracaju - SE
                    normalize_str("Edmilson Brito Rodrigues"),  # Belém -  PA
                    normalize_str("Alexandre Kalil"),  # Belo Horizonte - MG
                    normalize_str("Arthur Henrique Brandão Machado"),  # Boa Vista - RR
                    normalize_str("Marcos Marcello Trad"),  # Campo Grande - MS
                    normalize_str("Emanuel Pinheiro"),  # Cuiabá - MT
                    normalize_str("Rafael Valdomiro Greca de Macedo"),  # Curitiba - PR
                    normalize_str("Gean Marques Loureiro"),  # Florianópolis - SC
                    normalize_str("Jose Sarto Nogueira Moreira"),  # Fortaleza - CE
                    normalize_str("Luíz Alberto Maguito Vilela"),  # Goiânia - GO
                    normalize_str("Cícero de Lucena Filho"),  # João Pessoa - PB
                    normalize_str("Antônio Paulo de Oliveira Furlan"),  # Macapá - AP
                    normalize_str("João Henrique Holanda Caldas"),  # Maceió - AL
                    normalize_str("David Antônio Abisai Pereira de Almeida"),  # Manaus - AM
                    normalize_str("Álvaro Costa Dias"),  # Natal - RN
                    normalize_str("Cinthia Alves Caetano Ribeiro"),  # Palmas - TO
                    normalize_str("Sebastião de Araújo Melo"),  # Porto Alegre - RS
                    normalize_str("Hildon De Lima Chaves"),  # Porto Velho - RO
                    normalize_str("João Henrique de Andrade Lima Campos"),  # Recife - PE,
                    normalize_str("Sebastião Bocalom Rodrigues"),  # Rio Branco - AC
                    normalize_str("Eduardo da Costa Paes"),  # Rio de Janeiro - RJ
                    normalize_str("Bruno Soares Reis"),  # Salvador - BA
                    normalize_str("Eduardo Salim Braide"),  # São Luís - MA
                    normalize_str("Bruno Covas Lopes"),  # São Paulo - SP
                    normalize_str("José Pessoa Leal"),  # Teresina - PI
                    normalize_str("Lorenzo Silva de Pazolini"),  # Vitória - ES
                )
                df = df[df["NM_PRESTADOR_CONTA"].isin(mayors)]

        elif filetype == 'socios':
            valid_CPFs = kwargs["valid_CPFs"]
            """
            Os arquivos disponibilizados pela RFBR não têm cabeçalhos, portanto é necessário
            consultá-los no link:
            https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf
            """
            for file in Path(folder).glob(f"*.{file_ext}"):
                for chunk in pd.read_csv(
                    file,
                    sep=";",
                    encoding="latin-1",
                    names=[
                        "cnpj_basico",
                        "identificador_socio",
                        "nome_socio",
                        "cpf_socio",
                        "qualificacao_socio",
                        "data_entrada_sociedade",
                        "pais",
                        "cpf_representante_legal",
                        "nome_representante_legal",
                        "qualificacao_representante",
                        "faixa_etaria",
                    ],
                    dtype={
                        "cnpj_basico": str,
                        "cpf_socio": str,
                        "data_entrada_sociedade": int,
                    },
                    usecols=[
                        "cnpj_basico",
                        "cpf_socio",
                        "data_entrada_sociedade",
                        "nome_socio",
                    ],
                    chunksize=500_000,
                ):
                    chunk = chunk[chunk["cpf_socio"].isin(valid_CPFs)]
                    files_loaded.append(chunk)
            df = pd.concat(files_loaded)

    return df


def create_gephi_graph(df: pd.DataFrame, clean=True, **kwargs) -> None:

    if clean:
        df = df[
            (df["NOME_DOADOR"] != normalize_str("DOCUMENTO EXIGE RECUPERACAO MANUAL"))
            & (df["NOME_DOADOR"] != normalize_str("#NULO#"))
            & (df["NOME_DOADOR"] != normalize_str("DEBITO TED VIA STR MESMO TITULAR"))
            & (df["NOME_DOADOR"] != normalize_str("CREDITO DE REVERSAO DE TED"))
            & (df["NOME_DOADOR"] != normalize_str("DEBITO DE DOC ELETRONICO"))
            & (df["NOME_DOADOR"] != normalize_str("DEVOLUCAO DE TED"))
        ]

    if kwargs.get("single_donor"):
        cpf_donor = kwargs.get("single_donor")
        filename = f"{cpf_donor}.gdf"
        donors = df[df["CPF_CNPJ_DOADOR"] == cpf_donor]["NOME_DOADOR"]
        df = df[df["NOME_DOADOR"].isin(donors)]

    if kwargs.get("multiple_donors"):
        cpf_donors = kwargs.get("multiple_donors")
        filename = f"mutiple_donors.gdf"
        donors = df[df["CPF_CNPJ_DOADOR"].isin(cpf_donors)]["NOME_DOADOR"]

        df = df[df["NOME_DOADOR"].isin(donors)]

    else:
        print("não tem doadores específicos")
        filename = "graph.gdf"

    with open(filename, mode="w", encoding="utf-8") as f:
        f.write("nodedef>name VARCHAR,label VARCHAR, sigla_partido VARCHAR\n")
        f.write(
            "edgedef>source VARCHAR,target VARCHAR, sigla_partido VARCHAR, weight DOUBLE, directed BOOLEAN\n"
        )
        for node in df.itertuples():
            source = node.CPF_CNPJ_DOADOR
            target = node.CPF_CNPJ_CANDIDATO
            sigla_partido = node.SIGLA_PARTIDO
            valor = node.VALOR
            f.write(f"{source}, {target}, {sigla_partido}, {valor}, true\n")
