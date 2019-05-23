package pl.derezinski.allegro;

import pl.derezinski.allegro.mappers.CurrentStructureCompanyMapper;
import pl.derezinski.allegro.mappers.CurrentStructureUserMapper;
import pl.derezinski.allegro.models.CurrentStructureCompany;
import pl.derezinski.allegro.models.CurrentStructureUser;

import java.sql.*;
import java.util.*;

public class Main {

    /**
     * I present you my solution of the task. I had to make some assumptions, since some parts of the task were
     * not clear to me. So the solution is my interpretation of the task. I am aware of some weak points of my
     * solution, however I do hope that you will find it interesting and that you will see some value in it.
     * In case you have some additional questions to my solution, do not hesitate to contact me and I will be happy
     * to answer.
     * In order to perform the task, I have used Java, MySQL and JDBC.
     * Please provide your own password in the return statement of the get() method in the PasswordGetter class.
     */

    static Scanner scanner = new Scanner(System.in);
    static final int INITIAL_USER_ID_COUNTER = 1001;
    static int userIdCounter = INITIAL_USER_ID_COUNTER;
    static int deactivationCounter = 1;

    public static void main(String[] args) {
        runWithJDBC();
    }

    private static void runWithJDBC() {
        Connection connection = null;
        try {
            connection = establishConnection();

            System.out.println("  -->  Allegro - Developer (CRM) - Intern  <--");

            /**
             * Creating a table in the database: current_structure_private_user. The table represents the current
             * non-hierarchical structure of private users accounts. Some exemplary users are added to the table.
             * This is the preparation of the database that is necessary to perform the main task, ie. automatic
             * joining and transferring accounts to the new hierarchical structure.
             */
            createCurrentStructurePrivateUserTable(connection);

            /**
             * Similarly, current_structure_company table is created and some exemplary companies are added
             * to the table.
             */
            createCurrentStructureCompanyTable(connection);

            /**
             * Creating two tables in the database:
             *   - new_structure_private_user
             *   - new_structure_pr_user_accounts
             * They represent the new hierarchical structure of private users accounts. The first table will store
             * individual non-duplicated users; I added 'userid' column that serves as a primary key. The second table
             * will store all accounts with 'email' serving as a primary key (since email is unique for each account).
             * The table also contains 'userid' column as a foreign key pointing at the specific user.
             * I made an assumption that first name and last name are unambiguously referring to the same user
             * (the same person).
             */
            connection.setAutoCommit(false);
            boolean isEverythingCorrectSoFar = true;
            createNewStructurePrivateUserTables(connection);

            /**
             * Similarly, two tables are created in the database:
             *   - new_structure_company
             *   - new_structure_company_accounts
             * The first table will store individual non-duplicated companies with 'numbernip' serving as a primary
             * key (since NIP number is unique for each company and each company has a NIP number). The second table
             * will store all accounts with 'email' serving as a primary key (since email is unique for each account).
             * The table also contains 'numbernip' column as a foreign key pointing at the specific company.
             */
            createNewStructureCompanyTables(connection);

            /**
             * This is where magic begins to happen. All user accounts from the current non-hierarchical structure
             * are mapped and stored in the currentStructureUserList list.
             */
            Statement statement_1 = connection.createStatement();
            ResultSet resultSet_1 = statement_1.executeQuery("select * from current_structure_private_user");
            List<CurrentStructureUser> currentStructureUserList = new ArrayList<>();
            while (resultSet_1.next()) {
                CurrentStructureUserMapper currentStructureUserMapper = new CurrentStructureUserMapper();
                CurrentStructureUser currentStructureUser = currentStructureUserMapper.map(resultSet_1);
                currentStructureUserList.add(currentStructureUser);
            }
            resultSet_1.close();
            statement_1.close();
            Statement statement_2 = connection.createStatement();
            ResultSet resultSet_2 = statement_2.executeQuery("select count(*) from current_structure_private_user");
            resultSet_2.next();
            int totalNumberOfCurrentUserAccounts = resultSet_2.getInt("count(*)");
            resultSet_2.close();
            statement_2.close();
            if (currentStructureUserList.size() == totalNumberOfCurrentUserAccounts) {
                System.out.println("Data from table current_structure_private_user has been successfully mapped.");
            } else {
                System.out.println("Data from table current_structure_private_user has not been mapped.");
                isEverythingCorrectSoFar = false;
            }

            /**
             * Similarly, all company accounts from the current non-hierarchical structure are mapped and stored
             * in the currentStructureCompanyList list.
             */
            Statement statement_3 = connection.createStatement();
            ResultSet resultSet_3 = statement_3.executeQuery("select * from current_structure_company");
            List<CurrentStructureCompany> currentStructureCompanyList = new ArrayList<>();
            while (resultSet_3.next()) {
                CurrentStructureCompanyMapper currentStructureCompanyMapper = new CurrentStructureCompanyMapper();
                CurrentStructureCompany currentStructureCompany = currentStructureCompanyMapper.map(resultSet_3);
                currentStructureCompanyList.add(currentStructureCompany);
            }
            resultSet_3.close();
            statement_3.close();
            Statement statement_4 = connection.createStatement();
            ResultSet resultSet_4 = statement_4.executeQuery("select count(*) from current_structure_company");
            resultSet_4.next();
            int totalNumberOfCurrentCompanyAccounts = resultSet_4.getInt("count(*)");
            resultSet_4.close();
            statement_4.close();
            if (currentStructureCompanyList.size() == totalNumberOfCurrentCompanyAccounts) {
                System.out.println("Data from table current_structure_company has been successfully mapped.");
            } else {
                System.out.println("Data from table current_structure_company has not been mapped.");
                isEverythingCorrectSoFar = false;
            }

            /**
             * And finally, all data stored in the currentStructureUserList list are correctly added to the new
             * hierarchical structure of user accounts.
             */
            Map<String, Integer> usersAddedToTheNewStructure = new HashMap<>();
            String template_1 = "INSERT INTO new_structure_private_user (userid, firstname, lastname) VALUES (?, ?, ?)";
            PreparedStatement preparedStatement_1 = connection.prepareStatement(template_1);
            String template_2 = "INSERT INTO new_structure_pr_user_accounts (userid, id, email, phone1, phone2, login, city, postalcode, address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement preparedStatement_2 = connection.prepareStatement(template_2);
            for (CurrentStructureUser currentStructureUser : currentStructureUserList) {
                String name = currentStructureUser.getFirstName() + " " + currentStructureUser.getLastName();
                if (usersAddedToTheNewStructure.containsKey(name)) {
                    int existingUserId = usersAddedToTheNewStructure.get(name);
                    preparedStatement_2.setInt(1, existingUserId);
                } else {
                    usersAddedToTheNewStructure.put(name, userIdCounter);
                    preparedStatement_1.setInt(1, userIdCounter);
                    preparedStatement_1.setString(2, currentStructureUser.getFirstName());
                    preparedStatement_1.setString(3, currentStructureUser.getLastName());
                    preparedStatement_1.addBatch();

                    preparedStatement_2.setInt(1, userIdCounter);

                    userIdCounter++;
                }
                preparedStatement_2.setString(2, currentStructureUser.getId());
                preparedStatement_2.setString(3, currentStructureUser.getEmail());
                preparedStatement_2.setString(4, currentStructureUser.getPhone_1());
                preparedStatement_2.setString(5, currentStructureUser.getPhone_2());
                preparedStatement_2.setString(6, currentStructureUser.getLogin());
                preparedStatement_2.setString(7, currentStructureUser.getCity());
                preparedStatement_2.setString(8, currentStructureUser.getPostalCode());
                preparedStatement_2.setString(9, currentStructureUser.getAddress());
                preparedStatement_2.addBatch();
            }
            int[] resultOfPreparedStatement_1 = preparedStatement_1.executeBatch();
            int[] resultOfPreparedStatement_2 = preparedStatement_2.executeBatch();
            preparedStatement_1.close();
            preparedStatement_2.close();
            if (resultOfPreparedStatement_2.length == currentStructureUserList.size() &&
                resultOfPreparedStatement_1.length == usersAddedToTheNewStructure.size()) {
                System.out.println("All data has been successfully transferred to the new structure of user accounts.");
            } else {
                System.out.println("Data has not been successfully transferred to the new structure of user accounts.");
                isEverythingCorrectSoFar = false;
            }

            /**
             * Similarly, all data stored in the currentStructureCompanyList list are correctly added to the new
             * hierarchical structure of company accounts.
             */
            Map<String, String> companiesAddedToTheNewStructure = new HashMap<>();
            String template_3 = "INSERT INTO new_structure_company (companyname, numbernip) VALUES (?, ?)";
            PreparedStatement preparedStatement_3 = connection.prepareStatement(template_3);
            String template_4 = "INSERT INTO new_structure_company_accounts (numbernip, id, firstname, lastname, email, phone1, phone2, login, city, postalcode, address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement preparedStatement_4 = connection.prepareStatement(template_4);
            for (CurrentStructureCompany currentStructureCompany : currentStructureCompanyList) {
                if (companiesAddedToTheNewStructure.containsKey(currentStructureCompany.getCompanyName())) {
                    String existingNumberNIP = companiesAddedToTheNewStructure.get(currentStructureCompany.getCompanyName());
                    preparedStatement_4.setString(1, existingNumberNIP);
                } else {
                    companiesAddedToTheNewStructure.put(currentStructureCompany.getCompanyName(), currentStructureCompany.getNumberNIP());
                    preparedStatement_3.setString(1, currentStructureCompany.getCompanyName());
                    preparedStatement_3.setString(2, currentStructureCompany.getNumberNIP());
                    preparedStatement_3.addBatch();

                    preparedStatement_4.setString(1, currentStructureCompany.getNumberNIP());
                }
                preparedStatement_4.setString(2, currentStructureCompany.getId());
                preparedStatement_4.setString(3, currentStructureCompany.getFirstName());
                preparedStatement_4.setString(4, currentStructureCompany.getLastName());
                preparedStatement_4.setString(5, currentStructureCompany.getEmail());
                preparedStatement_4.setString(6, currentStructureCompany.getPhone_1());
                preparedStatement_4.setString(7, currentStructureCompany.getPhone_2());
                preparedStatement_4.setString(8, currentStructureCompany.getLogin());
                preparedStatement_4.setString(9, currentStructureCompany.getCity());
                preparedStatement_4.setString(10, currentStructureCompany.getPostalCode());
                preparedStatement_4.setString(11, currentStructureCompany.getAddress());
                preparedStatement_4.addBatch();
            }
            int[] resultOfPreparedStatement_3 = preparedStatement_3.executeBatch();
            int[] resultOfPreparedStatement_4 = preparedStatement_4.executeBatch();
            preparedStatement_3.close();
            preparedStatement_4.close();
            if (resultOfPreparedStatement_4.length == currentStructureCompanyList.size() &&
                    resultOfPreparedStatement_3.length == companiesAddedToTheNewStructure.size()) {
                System.out.println("All data has been successfully transferred to the new structure of company accounts.");
            } else {
                System.out.println("Data has not been successfully transferred to the new structure of company accounts.");
                isEverythingCorrectSoFar = false;
            }

            if (isEverythingCorrectSoFar) {
                connection.commit();
            } else {
                connection.rollback();
            }

            /**
             * Once all data has been successfully transferred to the new structure of accounts, you can add new
             * accounts to the structure. They will be correctly added and the database of accounts will expand.
             */
            System.out.println("Now you can add new accounts to the new structure of accounts.");
            boolean isThereSomeNewAccountToAdd = true;
            while (isThereSomeNewAccountToAdd) {
                System.out.print("Choose an action. Press: " +
                        "\n  1 - to create a new private user" +
                        "\n  2 - to create a new company user" +
                        "\n  3 - to end the program" +
                        "\nYour choice: ");
                String chosenOption = scanner.nextLine();
                switch (chosenOption) {
                    case "1":
                        createNewPrivateUser(connection);
                        break;
                    case "2":
                        createNewCompanyUser(connection);
                        break;
                    case "3":
                        isThereSomeNewAccountToAdd = false;
                        break;
                    default:
                        System.out.println("There is no such option to choose.");
                        break;
                }
            }

            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createCurrentStructurePrivateUserTable(Connection connection) throws SQLException {
        Statement statement_1 = connection.createStatement();
        statement_1.execute("CREATE TABLE current_structure_private_user " +
                "(firstname VARCHAR(30) NOT NULL, " +
                "lastname VARCHAR(30) NOT NULL, " +
                "id INT, " +
                "email VARCHAR(40) NOT NULL, " +
                "phone1 CHAR(9) NOT NULL, " +
                "phone2 CHAR(9), " +
                "login VARCHAR(30) NOT NULL, " +
                "city VARCHAR(20) NOT NULL, " +
                "postalcode CHAR(6) NOT NULL, " +
                "address VARCHAR(40) NOT NULL)");
        statement_1.close();

        Statement statement_2 = connection.createStatement();
        statement_2.execute("ALTER TABLE current_structure_private_user " +
                "ADD CONSTRAINT pk_email PRIMARY KEY ( email )");
        statement_2.close();

        Statement statement_3 = connection.createStatement();
        statement_3.executeUpdate("INSERT INTO current_structure_private_user (firstname, lastname, id, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Daenerys', 'Targaryen', 101, 'daenerys.stormborn@ravenmail.com', '155010101', '155010102', 'motherofdragons', 'Meereen', '155-01', '155 Dothraki Street')");
        statement_3.close();

        Statement statement_4 = connection.createStatement();
        statement_4.executeUpdate("INSERT INTO current_structure_private_user (firstname, lastname, id, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Jaime', 'Lannister', 102, 'jlannister@ravenmail.com', '255010101', '255010102', 'kingslayer', 'Kings Landing', '255-01', '3 Castle Street')");
        statement_4.close();

        Statement statement_5 = connection.createStatement();
        statement_5.executeUpdate("INSERT INTO current_structure_private_user (firstname, lastname, id, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Cersei', 'Lannister', 103, 'cerseilannister@ravenmail.com', '255010103', '255010102', 'queenregent', 'Kings Landing', '255-01', '3 Castle Street')");
        statement_5.close();

        Statement statement_6 = connection.createStatement();
        statement_6.executeUpdate("INSERT INTO current_structure_private_user (firstname, lastname, id, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Petyr', 'Baelish', null, 'littlefinger33@ravenmail.com', '255010104', null, 'littlefinger', 'Kings Landing', '255-02', '10 Lannister Avenue')");
        statement_6.close();

        Statement statement_7 = connection.createStatement();
        statement_7.executeUpdate("INSERT INTO current_structure_private_user (firstname, lastname, id, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Eddard', 'Stark', null, 'lordofwinterfell@ravenmail.com', '355010101', null, 'ned', 'Winterfell', '355-01', '1 North Street')");
        statement_7.close();

        Statement statement_8 = connection.createStatement();
        statement_8.executeUpdate("INSERT INTO current_structure_private_user (firstname, lastname, id, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Catelyn', 'Stark', null, 'catelyn.stark@ravenmail.com', '355010102', null, 'ladystoneheart', 'Winterfell', '355-01', '1 North Street')");
        statement_8.close();

        Statement statement_9 = connection.createStatement();
        statement_9.executeUpdate("INSERT INTO current_structure_private_user (firstname, lastname, id, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Robb', 'Stark', null, 'stark2345@ravenmail.com', '355010103', '355222222', 'theyoungwolf', 'Winterfell', '355-01', '1 North Street')");
        statement_9.close();

        Statement statement_10 = connection.createStatement();
        statement_10.executeUpdate("INSERT INTO current_structure_private_user (firstname, lastname, id, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Sansa', 'Stark', null, 'sansasansa@ravenmail.com', '355010104', '355222222', 'sansa', 'Winterfell', '355-01', '1 North Street')");
        statement_10.close();

        Statement statement_11 = connection.createStatement();
        statement_11.executeUpdate("INSERT INTO current_structure_private_user (firstname, lastname, id, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Bran', 'Stark', null, 'threeeyedraven@ravenmail.com', '355010105', '355222222', 'bran1', 'Winterfell', '355-01', '1 North Street')");
        statement_11.close();

        Statement statement_12 = connection.createStatement();
        statement_12.executeUpdate("INSERT INTO current_structure_private_user (firstname, lastname, id, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Arya', 'Stark', null, 'aryastark667@ravenmail.com', '355010106', '355222222', 'noone', 'Winterfell', '355-01', '1 North Street')");
        statement_12.close();

        Statement statement_13 = connection.createStatement();
        statement_13.executeUpdate("INSERT INTO current_structure_private_user (firstname, lastname, id, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Olenna', 'Tyrell', 104, 'thequeenofthorns@ravenmail.com', '455010101', null, 'ladyolenna', 'Highgarden', '455-01', '24 Flower Street')");
        statement_13.close();

        Statement statement_14 = connection.createStatement();
        statement_14.executeUpdate("INSERT INTO current_structure_private_user (firstname, lastname, id, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Daenerys', 'Targaryen', null, 'daenerys222@ravenmail.com', '155010101', '155010102', 'khaleesi', 'Dothraki See', '555-01', '3 Black Street')");
        statement_14.close();
    }

    private static void createCurrentStructureCompanyTable(Connection connection) throws SQLException {
        Statement statement_1 = connection.createStatement();
        statement_1.execute("CREATE TABLE current_structure_company " +
                "(companyname VARCHAR(30) NOT NULL, " +
                "numbernip CHAR(13) NOT NULL, " +
                "id INT, " +
                "firstname VARCHAR(30) NOT NULL, " +
                "lastname VARCHAR(30) NOT NULL, " +
                "email VARCHAR(40) NOT NULL, " +
                "phone1 CHAR(9) NOT NULL, " +
                "phone2 CHAR(9), " +
                "login VARCHAR(30) NOT NULL, " +
                "city VARCHAR(20) NOT NULL, " +
                "postalcode CHAR(6) NOT NULL, " +
                "address VARCHAR(40) NOT NULL)");
        statement_1.close();

        Statement statement_2 = connection.createStatement();
        statement_2.execute("ALTER TABLE current_structure_company " +
                "ADD CONSTRAINT pk_email PRIMARY KEY ( email )");
        statement_2.close();

        Statement statement_3 = connection.createStatement();
        statement_3.executeUpdate("INSERT INTO current_structure_company (companyname, numbernip, id, firstname, lastname, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Nights Watch', '321-32-32-321', 101, 'Jon', 'Snow', 'thebastardofwinterfell@ravenmail.com', '655010101', '655010102', 'iknownothing', 'Castle Black', '655-01', '7 Wall Street')");
        statement_3.close();

        Statement statement_4 = connection.createStatement();
        statement_4.executeUpdate("INSERT INTO current_structure_company (companyname, numbernip, id, firstname, lastname, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Nights Watch', '321-32-32-321', 102, 'Aemon', 'Targaryen', 'maester.aemon@ravenmail.com', '655010103', null, 'maesteraemon', 'Castle Black', '655-01', '7 Wall Street')");
        statement_4.close();

        Statement statement_5 = connection.createStatement();
        statement_5.executeUpdate("INSERT INTO current_structure_company (companyname, numbernip, id, firstname, lastname, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Nights Watch', '321-32-32-321', 103, 'Jeor', 'Mormont', 'jmormont123@ravenmail.com', '655010103', null, 'jmormont', 'Castle Black', '655-01', '7 Wall Street')");
        statement_5.close();

        Statement statement_6 = connection.createStatement();
        statement_6.executeUpdate("INSERT INTO current_structure_company (companyname, numbernip, id, firstname, lastname, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Brotherhood Without Banners', '4324343432', null, 'Beric', 'Dondarrion', 'bericberic@ravenmail.com', '755010101', null, 'dondarrion', 'Riverlands', '755-01', '2 Resurrection Avenue')");
        statement_6.close();

        Statement statement_7 = connection.createStatement();
        statement_7.executeUpdate("INSERT INTO current_structure_company (companyname, numbernip, id, firstname, lastname, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Nights Watch', '321-32-32-321', 104, 'Samwell ', 'Tarly', 'sam001@ravenmail.com', '655010103', '855010101', 'samwelltarly', 'Citadel', '855-01', '47 Library Street')");
        statement_7.close();

        Statement statement_8 = connection.createStatement();
        statement_8.executeUpdate("INSERT INTO current_structure_company (companyname, numbernip, id, firstname, lastname, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Brotherhood Without Banners', '4324343432', null, 'Thoros', 'of Myr', 'thoros.of.myr@ravenmail.com', '755010102', null, 'thoros', 'Riverlands', '755-01', '2 Resurrection Avenue')");
        statement_8.close();

        Statement statement_9 = connection.createStatement();
        statement_9.executeUpdate("INSERT INTO current_structure_company (companyname, numbernip, id, firstname, lastname, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Brotherhood Without Banners', '4324343432', null, 'Lady', 'Stoneheart', 'formerlycatelynstark@ravenmail.com', '755010103', null, 'stoneheart', 'Riverlands', '755-01', '2 Resurrection Avenue')");
        statement_9.close();

        Statement statement_10 = connection.createStatement();
        statement_10.executeUpdate("INSERT INTO current_structure_company (companyname, numbernip, id, firstname, lastname, email, phone1, phone2, login, city, postalcode, address) " +
                "VALUES ('Iron Bank of Braavos', 'ES5435435432', null, 'Tycho', 'Nestoris', 'tycho.nestoris@ravenmail.com', '855010101', '855010102', 'tychonestoris', 'Braavos', '855-01', '1 Titan of Braavos Lane')");
        statement_10.close();
    }

    private static void createNewStructurePrivateUserTables(Connection connection) throws SQLException {
        Statement statement_1 = connection.createStatement();
        statement_1.execute("CREATE TABLE new_structure_pr_user_accounts " +
                "(userid INT NOT NULL, " +
                "id INT, " +
                "email VARCHAR(40) NOT NULL, " +
                "phone1 CHAR(9) NOT NULL, " +
                "phone2 CHAR(9), " +
                "login VARCHAR(30) NOT NULL, " +
                "city VARCHAR(20) NOT NULL, " +
                "postalcode CHAR(6) NOT NULL, " +
                "address VARCHAR(40) NOT NULL)");
        statement_1.close();

        Statement statement_2 = connection.createStatement();
        statement_2.execute("ALTER TABLE new_structure_pr_user_accounts " +
                "ADD CONSTRAINT pk_new_str_email PRIMARY KEY ( email )");
        statement_2.close();

        Statement statement_3 = connection.createStatement();
        statement_3.execute("CREATE TABLE new_structure_private_user " +
                "(userid INT NOT NULL, " +
                "firstname VARCHAR(30) NOT NULL, " +
                "lastname VARCHAR(30) NOT NULL)");
        statement_3.close();

        Statement statement_4 = connection.createStatement();
        statement_4.execute("ALTER TABLE new_structure_private_user " +
                "ADD CONSTRAINT pk_userid PRIMARY KEY ( userid )");
        statement_4.close();

        Statement statement_5 = connection.createStatement();
        statement_5.execute("ALTER TABLE new_structure_pr_user_accounts " +
                "ADD CONSTRAINT foreign_key_userid FOREIGN KEY ( userid ) REFERENCES new_structure_private_user ( userid )");
        statement_5.close();
    }

    private static void createNewStructureCompanyTables(Connection connection) throws SQLException {
        Statement statement_1 = connection.createStatement();
        statement_1.execute("CREATE TABLE new_structure_company_accounts " +
                "(numbernip CHAR(13) NOT NULL, " +
                "id INT, " +
                "firstname VARCHAR(30) NOT NULL, " +
                "lastname VARCHAR(30) NOT NULL, " +
                "email VARCHAR(40) NOT NULL, " +
                "phone1 CHAR(9) NOT NULL, " +
                "phone2 CHAR(9), " +
                "login VARCHAR(30) NOT NULL, " +
                "city VARCHAR(20) NOT NULL, " +
                "postalcode CHAR(6) NOT NULL, " +
                "address VARCHAR(40) NOT NULL)");
        statement_1.close();

        Statement statement_2 = connection.createStatement();
        statement_2.execute("ALTER TABLE new_structure_company_accounts " +
                "ADD CONSTRAINT pk_new_str_email PRIMARY KEY ( email )");
        statement_2.close();

        Statement statement_3 = connection.createStatement();
        statement_3.execute("CREATE TABLE new_structure_company " +
                "(companyname VARCHAR(30) NOT NULL, " +
                "numbernip CHAR(13) NOT NULL)");
        statement_3.close();

        Statement statement_4 = connection.createStatement();
        statement_4.execute("ALTER TABLE new_structure_company " +
                "ADD CONSTRAINT pk_numbernip PRIMARY KEY ( numbernip )");
        statement_4.close();

        Statement statement_5 = connection.createStatement();
        statement_5.execute("ALTER TABLE new_structure_company_accounts " +
                "ADD CONSTRAINT foreign_key_numbernip FOREIGN KEY ( numbernip ) REFERENCES new_structure_company ( numbernip )");
        statement_5.close();
    }

    private static void createNewPrivateUser(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        System.out.print("Enter an email: ");
        String email = scanner.nextLine();
        String template = "select * from new_structure_pr_user_accounts where email = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(template);
        preparedStatement.setString(1, email);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            System.out.print("The email already exists in the database. Press: " +
                    "\n  A - to continue (this will deactivate the account that currently has this email)" +
                    "\n  B - to quit" +
                    "\nYour choice: ");
            String chosenOption = scanner.nextLine();
            if (chosenOption.equals("A")) {
                String template_2 = "UPDATE new_structure_pr_user_accounts SET email = ? WHERE email = ?";
                PreparedStatement preparedStatement_2 = connection.prepareStatement(template_2);
                preparedStatement_2.setString(1, "deactivated" + deactivationCounter);
                deactivationCounter++;
                preparedStatement_2.setString(2, email);
                int result = preparedStatement_2.executeUpdate();
                if (result == 1) {
                    System.out.println("The old account has been successfully deactivated.");
                }
                preparedStatement_2.close();
            } else {
                return;
            }
        }
        resultSet.close();
        preparedStatement.close();
        System.out.print("Enter first name: ");
        String firstName = scanner.nextLine();
        System.out.print("Enter last name: ");
        String lastName = scanner.nextLine();
        System.out.print("Enter first phone number: ");
        String phone1 = scanner.nextLine();
        System.out.print("Enter second phone number: ");
        String phone2 = scanner.nextLine();
        System.out.print("Enter login: ");
        String login = scanner.nextLine();
        System.out.print("Enter city: ");
        String city = scanner.nextLine();
        System.out.print("Enter postal code: ");
        String postalCode = scanner.nextLine();
        System.out.print("Enter address: ");
        String address = scanner.nextLine();

        String template_3 = "INSERT INTO new_structure_private_user (userid, firstname, lastname) VALUES (?, ?, ?)";
        PreparedStatement preparedStatement_3 = connection.prepareStatement(template_3);
        String template_4 = "INSERT INTO new_structure_pr_user_accounts (userid, id, email, phone1, phone2, login, city, postalcode, address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement preparedStatement_4 = connection.prepareStatement(template_4);
        String template_5 = "SELECT * from new_structure_private_user where firstname = ? and lastname = ?";
        PreparedStatement preparedStatement_5 = connection.prepareStatement(template_5);
        preparedStatement_5.setString(1, firstName);
        preparedStatement_5.setString(2, lastName);
        ResultSet resultSet_2 = preparedStatement_5.executeQuery();
        int result_1 = 1;
        if (resultSet_2.next()) {
            int existingUserId = resultSet_2.getInt(1);
            preparedStatement_4.setInt(1, existingUserId);
        } else {
            preparedStatement_3.setInt(1, userIdCounter);
            preparedStatement_3.setString(2, firstName);
            preparedStatement_3.setString(3, lastName);
            result_1 = preparedStatement_3.executeUpdate();

            preparedStatement_4.setInt(1, userIdCounter);

            userIdCounter++;
        }
        preparedStatement_4.setString(2, null);
        preparedStatement_4.setString(3, email);
        preparedStatement_4.setString(4, phone1);
        preparedStatement_4.setString(5, phone2);
        preparedStatement_4.setString(6, login);
        preparedStatement_4.setString(7, city);
        preparedStatement_4.setString(8, postalCode);
        preparedStatement_4.setString(9, address);
        int result_2 = preparedStatement_4.executeUpdate();
        resultSet_2.close();
        preparedStatement_3.close();
        preparedStatement_4.close();
        preparedStatement_5.close();
        if (result_1 == 1 && result_2 == 1) {
            System.out.println("New private user account has been successfully created.");
            connection.commit();
        } else {
            System.out.println("New private user account has not been created.");
            connection.rollback();
        }
    }

    private static void createNewCompanyUser(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        System.out.print("Enter an email: ");
        String email = scanner.nextLine();
        String template = "select * from new_structure_company_accounts where email = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(template);
        preparedStatement.setString(1, email);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            System.out.print("The email already exists in the database. Press: " +
                    "\n  A - to continue (this will deactivate the account that currently has this email)" +
                    "\n  B - to quit" +
                    "\nYour choice: ");
            String chosenOption = scanner.nextLine();
            if (chosenOption.equals("A")) {
                String template_2 = "UPDATE new_structure_company_accounts SET email = ? WHERE email = ?";
                PreparedStatement preparedStatement_2 = connection.prepareStatement(template_2);
                preparedStatement_2.setString(1, "deactivated" + deactivationCounter);
                deactivationCounter++;
                preparedStatement_2.setString(2, email);
                int result = preparedStatement_2.executeUpdate();
                if (result == 1) {
                    System.out.println("The old account has been successfully deactivated.");
                }
                preparedStatement_2.close();
            } else {
                return;
            }
        }
        resultSet.close();
        preparedStatement.close();
        System.out.print("Enter company name: ");
        String companyName = scanner.nextLine();
        System.out.print("Enter NIP number: ");
        String numberNIP = scanner.nextLine();
        System.out.print("Enter first name: ");
        String firstName = scanner.nextLine();
        System.out.print("Enter last name: ");
        String lastName = scanner.nextLine();
        System.out.print("Enter first phone number: ");
        String phone1 = scanner.nextLine();
        System.out.print("Enter second phone number: ");
        String phone2 = scanner.nextLine();
        System.out.print("Enter login: ");
        String login = scanner.nextLine();
        System.out.print("Enter city: ");
        String city = scanner.nextLine();
        System.out.print("Enter postal code: ");
        String postalCode = scanner.nextLine();
        System.out.print("Enter address: ");
        String address = scanner.nextLine();

        String template_3 = "INSERT INTO new_structure_company (companyname, numbernip) VALUES (?, ?)";
        PreparedStatement preparedStatement_3 = connection.prepareStatement(template_3);
        String template_4 = "INSERT INTO new_structure_company_accounts (numbernip, id, firstname, lastname, email, phone1, phone2, login, city, postalcode, address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement preparedStatement_4 = connection.prepareStatement(template_4);
        String template_5 = "SELECT * from new_structure_company where numbernip = ?";
        PreparedStatement preparedStatement_5 = connection.prepareStatement(template_5);
        preparedStatement_5.setString(1, numberNIP);
        ResultSet resultSet_2 = preparedStatement_5.executeQuery();
        int result_1 = 1;
        if (resultSet_2.next()) {
            String existingNumberNIP = resultSet_2.getString(2);
            preparedStatement_4.setString(1, existingNumberNIP);
        } else {
            preparedStatement_3.setString(1, companyName);
            preparedStatement_3.setString(2, numberNIP);
            result_1 = preparedStatement_3.executeUpdate();

            preparedStatement_4.setString(1, numberNIP);
        }
        preparedStatement_4.setString(2, null);
        preparedStatement_4.setString(3, firstName);
        preparedStatement_4.setString(4, lastName);
        preparedStatement_4.setString(5, email);
        preparedStatement_4.setString(6, phone1);
        preparedStatement_4.setString(7, phone2);
        preparedStatement_4.setString(8, login);
        preparedStatement_4.setString(9, city);
        preparedStatement_4.setString(10, postalCode);
        preparedStatement_4.setString(11, address);
        int result_2 = preparedStatement_4.executeUpdate();
        resultSet_2.close();
        preparedStatement_3.close();
        preparedStatement_4.close();
        preparedStatement_5.close();
        if (result_1 == 1 && result_2 == 1) {
            System.out.println("New company user account has been successfully created.");
            connection.commit();
        } else {
            System.out.println("New company user account has not been created.");
            connection.rollback();
        }
    }

    private static Connection establishConnection() throws SQLException {
        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", PasswordGetter.get());
        return DriverManager.getConnection("jdbc:mysql://localhost:3306/world?serverTimezone=GMT",
                properties);
    }

}
