"""
This script, generer_data.py, is designed to create and populate a Microsoft SQL Server database table with realistic 
data for cereals genetics. It includes functions to establish a database connection, create a table, 
generate realistic data, and insert the data into the table. The script uses the pyodbc library 
for database operations and numpy for numerical operations. The generated data includes various attributes related 
to cereals, such as species, genetic type, origin, and yield per hectare. 
This script is useful for setting up a test database with realistic data for development and testing purposes.
    """

""""""
import pyodbc
import random
from datetime import datetime, timedelta
import numpy as np


def create_database_connection(
    server="localhost",
    database="master",
    username="sa",
    password="MyComplexPassword123!",
):
    """Create and return a database connection"""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        return conn
    except pyodbc.Error as e:
        print(f"Error connecting to database: {str(e)}")
        raise


def create_table(cursor):
    """Create the Cereales_Genetics_Advanced table"""
    try:
        # Check if table exists
        cursor.execute(
            """
            IF EXISTS (SELECT * FROM sys.objects 
                WHERE object_id = OBJECT_ID(N'Cereales_Genetics_Advanced') 
                AND type in (N'U'))
            DROP TABLE Cereales_Genetics_Advanced
        """
        )

        create_table_query = """
        CREATE TABLE Cereales_Genetics_Advanced (
            Id INT IDENTITY(1,1) PRIMARY KEY,
            Nom_Variete NVARCHAR(100),
            Code_Variete NVARCHAR(50),
            Espece NVARCHAR(50),
            Type_Genetique NVARCHAR(100),
            Caractere_Genetique NVARCHAR(255),
            Origine NVARCHAR(100),
            Date_Creation DATE,
            Statut_Commercialisation NVARCHAR(50),
            Produit_Lab NVARCHAR(100),
            Resultats_Etudes NVARCHAR(MAX),
            Date_Mise_A_Jour DATETIME2,
            Taille_Plant FLOAT,
            Couleur_Epi NVARCHAR(50),
            Date_Maturation DATE,
            Taux_Proteine DECIMAL(5,2),
            Rendement_Par_Hectare DECIMAL(10,2),
            Resilience_Sol DECIMAL(5,2),
            Resistance_Temperature DECIMAL(5,2),
            Temps_Pousse INT,
            Type_Environnement NVARCHAR(100),
            Age_Variete INT,
            Nombre_Epi INT,
            Longueur_Epi FLOAT,
            Densite_Plante INT,
            Type_Engrais NVARCHAR(50),
            Plante_Compagnon NVARCHAR(100),
            Densite_Meteo DECIMAL(5,2),
            Resistance_Maladies NVARCHAR(100),
            Taux_Humidite DECIMAL(5,2),
            Impact_Chimique NVARCHAR(50),
            Technologie_Utilisee NVARCHAR(100),
            Zone_Predominante NVARCHAR(100),
            Description_Produit NVARCHAR(255),
            Type_Utilisation NVARCHAR(100),
            Performance_Climatique NVARCHAR(100),
            Type_Culture NVARCHAR(50),
            Qualite_Semence NVARCHAR(50),
            Durabilite NVARCHAR(100),
            Strategie_Amelioration NVARCHAR(100),
            Taux_Produit_OGM NVARCHAR(50)
        )
        """
        cursor.execute(create_table_query)
        cursor.commit()
        print("Table created successfully")
    except pyodbc.Error as e:
        print(f"Error creating table: {str(e)}")
        raise


def generate_realistic_data(num_records=100):
    """Generate realistic data for cereals genetics with complete information"""
    especes = ["Blé tendre", "Blé dur", "Orge", "Avoine", "Seigle", "Triticale"]
    types_genetiques = [
        "Hybride F1",
        "Lignée pure",
        "Population",
        "Composite",
        "Synthétique",
    ]
    origines = [
        "France",
        "Allemagne",
        "Pays-Bas",
        "Belgique",
        "Italie",
        "Espagne",
        "États-Unis",
    ]
    statuts = [
        "En développement",
        "Commercialisé",
        "En test",
        "En attente d'homologation",
    ]
    laboratoires = [
        "INRAE",
        "Limagrain",
        "KWS",
        "Syngenta",
        "RAGT",
        "Florimond Desprez",
    ]
    couleurs_epi = ["Jaune paille", "Blanc", "Roux", "Doré", "Brun"]
    types_engrais = ["Azote organique", "NPK", "Urée", "Ammonitrate", "Fumier composté"]
    plantes_compagnon = ["Trèfle", "Luzerne", "Vesce", "Pois", "Féverole"]
    impacts_chimiques = ["Faible", "Modéré", "Élevé"]
    types_culture = ["Conventionnel", "Biologique", "Raisonné", "Conservation"]
    qualites_semence = ["Premium", "Standard", "Certifiée", "Elite"]

    data = []
    base_date = datetime(2020, 1, 1)

    for i in range(1, num_records + 1):
        espece = random.choice(especes)
        code_prefix = espece[:2].upper()

        # Générer des résultats d'études réalistes
        resultats_etudes = f"""
        Essai {i} :
        - Rendement moyen: {random.uniform(60, 90):.1f} q/ha
        - Taux de germination: {random.uniform(85, 98):.1f}%
        - Résistance maladie: {random.choice(['Excellente', 'Bonne', 'Moyenne'])}
        - Test en conditions réelles: {random.choice(['Positif', 'En cours', 'À confirmer'])}
        """

        record = {
            "Nom_Variete": f"{espece}-{random.randint(1000, 9999)}",
            "Code_Variete": f"{code_prefix}{random.randint(100, 999)}",
            "Espece": espece,
            "Type_Genetique": random.choice(types_genetiques),
            "Caractere_Genetique": random.choice(
                [
                    "Résistance maladies++",
                    "Haut rendement",
                    "Tolérance sécheresse",
                    "Qualité protéique",
                    "Adaptation climat",
                ]
            ),
            "Origine": random.choice(origines),
            "Date_Creation": base_date + timedelta(days=random.randint(0, 365)),
            "Statut_Commercialisation": random.choice(statuts),
            "Produit_Lab": random.choice(laboratoires),
            "Resultats_Etudes": resultats_etudes,
            "Date_Mise_A_Jour": datetime.now(),
            "Taille_Plant": round(random.uniform(0.8, 2.0), 2),
            "Couleur_Epi": random.choice(couleurs_epi),
            "Date_Maturation": base_date + timedelta(days=random.randint(150, 200)),
            "Taux_Proteine": round(random.uniform(10.0, 16.0), 2),
            "Rendement_Par_Hectare": round(random.uniform(60.0, 120.0), 2),
            "Resilience_Sol": round(random.uniform(3.0, 7.0), 2),
            "Resistance_Temperature": round(random.uniform(15.0, 35.0), 2),
            "Temps_Pousse": random.randint(140, 180),
            "Type_Environnement": random.choice(
                ["Plein champ", "Serre", "Semi-protégé"]
            ),
            "Age_Variete": random.randint(1, 5),
            "Nombre_Epi": random.randint(2, 6),
            "Longueur_Epi": round(random.uniform(8.0, 15.0), 1),
            "Densite_Plante": random.randint(200, 400),
            "Type_Engrais": random.choice(types_engrais),
            "Plante_Compagnon": random.choice(plantes_compagnon),
            "Densite_Meteo": round(random.uniform(0.8, 1.2), 2),
            "Resistance_Maladies": random.choice(["Élevée", "Moyenne", "Faible"]),
            "Taux_Humidite": round(random.uniform(12.0, 15.0), 2),
            "Impact_Chimique": random.choice(impacts_chimiques),
            "Technologie_Utilisee": random.choice(
                [
                    "Sélection assistée par marqueurs",
                    "Culture in vitro",
                    "Hybridation classique",
                ]
            ),
            "Zone_Predominante": random.choice(
                ["Nord", "Sud", "Est", "Ouest", "Centre"]
            ),
            "Description_Produit": f'Variété de {espece} adaptée pour {random.choice(["climat continental", "climat océanique", "climat méditerranéen"])}',
            "Type_Utilisation": random.choice(
                ["Alimentation humaine", "Alimentation animale", "Double usage"]
            ),
            "Performance_Climatique": random.choice(["Excellente", "Bonne", "Moyenne"]),
            "Type_Culture": random.choice(types_culture),
            "Qualite_Semence": random.choice(qualites_semence),
            "Durabilite": f"{random.randint(2, 5)} ans",
            "Strategie_Amelioration": random.choice(
                ["Sélection récurrente", "Rétrocroisement", "Sélection généalogique"]
            ),
            "Taux_Produit_OGM": f"{random.randint(0, 5)}%",
        }
        data.append(record)

    return data


def insert_data(cursor, data):
    """Insert complete data into the database"""
    try:
        insert_query = """
        INSERT INTO Cereales_Genetics_Advanced (
            Nom_Variete, Code_Variete, Espece, Type_Genetique, Caractere_Genetique,
            Origine, Date_Creation, Statut_Commercialisation, Produit_Lab, Resultats_Etudes,
            Date_Mise_A_Jour, Taille_Plant, Couleur_Epi, Date_Maturation, Taux_Proteine,
            Rendement_Par_Hectare, Resilience_Sol, Resistance_Temperature, Temps_Pousse,
            Type_Environnement, Age_Variete, Nombre_Epi, Longueur_Epi, Densite_Plante,
            Type_Engrais, Plante_Compagnon, Densite_Meteo, Resistance_Maladies, Taux_Humidite,
            Impact_Chimique, Technologie_Utilisee, Zone_Predominante, Description_Produit,
            Type_Utilisation, Performance_Climatique, Type_Culture, Qualite_Semence, Durabilite,
            Strategie_Amelioration, Taux_Produit_OGM
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for record in data:
            cursor.execute(
                insert_query,
                (
                    record["Nom_Variete"],
                    record["Code_Variete"],
                    record["Espece"],
                    record["Type_Genetique"],
                    record["Caractere_Genetique"],
                    record["Origine"],
                    record["Date_Creation"],
                    record["Statut_Commercialisation"],
                    record["Produit_Lab"],
                    record["Resultats_Etudes"],
                    record["Date_Mise_A_Jour"],
                    record["Taille_Plant"],
                    record["Couleur_Epi"],
                    record["Date_Maturation"],
                    record["Taux_Proteine"],
                    record["Rendement_Par_Hectare"],
                    record["Resilience_Sol"],
                    record["Resistance_Temperature"],
                    record["Temps_Pousse"],
                    record["Type_Environnement"],
                    record["Age_Variete"],
                    record["Nombre_Epi"],
                    record["Longueur_Epi"],
                    record["Densite_Plante"],
                    record["Type_Engrais"],
                    record["Plante_Compagnon"],
                    record["Densite_Meteo"],
                    record["Resistance_Maladies"],
                    record["Taux_Humidite"],
                    record["Impact_Chimique"],
                    record["Technologie_Utilisee"],
                    record["Zone_Predominante"],
                    record["Description_Produit"],
                    record["Type_Utilisation"],
                    record["Performance_Climatique"],
                    record["Type_Culture"],
                    record["Qualite_Semence"],
                    record["Durabilite"],
                    record["Strategie_Amelioration"],
                    record["Taux_Produit_OGM"],
                ),
            )

        cursor.commit()
        print(f"{len(data)} records inserted successfully")
    except pyodbc.Error as e:
        print(f"Error inserting data: {str(e)}")
        raise


def main():
    try:
        # Create connection
        conn = create_database_connection()
        cursor = conn.cursor()

        # Create table
        create_table(cursor)

        # Generate and insert data
        data = generate_realistic_data(100)
        insert_data(cursor, data)

        # Close connection
        cursor.close()
        conn.close()
        print("Process completed successfully!")

    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
