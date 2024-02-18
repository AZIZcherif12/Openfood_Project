package openfood; // Déclaration du package

// Import des classes nécessaires
import java.sql.*;
import org.bson.Document;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class openfood {

    public static void main(String[] args) throws SQLException {
        // Informations de connexion à la base de données MySQL
        String mysqlUrl = "jdbc:mysql://localhost:3306/openfood";
        String mysqlUser = "openfood";
        
        // URI de connexion à la base de données MongoDB
        String uri = "mongodb://127.0.0.1/";
        
        // Bloc try-with-resources pour assurer la fermeture automatique des ressources
        try (Connection conn = DriverManager.getConnection(mysqlUrl, mysqlUser, "password")) {
            try (MongoClient mongoClient = MongoClients.create(uri)) {  
                // Accès à la base de données MongoDB et à la collection DATA
                MongoDatabase database = mongoClient.getDatabase("FOOD_DB");
                MongoCollection<Document> collection = database.getCollection("DATA");
                
                // Exécution d'une requête sur la base MySQL
                try (Statement stmt = conn.createStatement()) {
                    // Itération sur les documents de la collection MongoDB
                    MongoCursor<Document> cursor = collection.find().iterator();
                    while (cursor.hasNext()) {
                        Document doc = cursor.next();
                        
                        // Récupération des valeurs du document
                        Object productName = doc.get("product_name");
                        String productNameString = null;
                        if(productName != null) {
                            productNameString = productName.toString(); 
                        }
                        
                        Object quantity = doc.get("quantity");
                        String quantityString = null;
                        if(quantity != null) {
                            quantityString = quantity.toString();
                        }
                        
                        // Récupération des nutriments du document et insertion dans MySQL
                        Document nutriments = (Document) doc.get("nutriments");
                        String energyValueString = null;
                        if(nutriments != null) {
                            energyValueString = getStringValue(nutriments.get("energy_value"));
                        }
                        
                        // Conversion du document nutriments en chaîne JSON
                        String nutrimentsString = (nutriments != null) ? nutriments.toJson() : null;
                        
                        // Requête SQL d'insertion dans MySQL
                        String sqlQuery = "INSERT INTO products (product_name, quantity, nutriments, energy_value) VALUES (?, ?, ?, ?)";
                        try (PreparedStatement statement = conn.prepareStatement(sqlQuery)) {
                            statement.setString(1, productNameString);
                            statement.setString(2, quantityString);
                            statement.setString(3, nutrimentsString);
                            statement.setString(4, energyValueString);
                            statement.executeUpdate();
                            System.out.println(sqlQuery);
                        }
                    }
                    cursor.close();
                }
            } 
        }
    }

    // Méthode pour obtenir une représentation sous forme de chaîne de caractères d'une valeur
    private static String getStringValue(Object obj) {
        if (obj instanceof Double || obj instanceof Integer) {
            return String.valueOf(obj);
        } else {
            System.out.println("Invalid value.");
            return ""; // Ou gérer le cas invalide de manière appropriée
        }
    }
}
