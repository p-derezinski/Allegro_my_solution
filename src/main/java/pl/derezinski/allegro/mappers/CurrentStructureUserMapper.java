package pl.derezinski.allegro.mappers;

import pl.derezinski.allegro.models.CurrentStructureUser;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CurrentStructureUserMapper {

    public CurrentStructureUser map(ResultSet resultSet) throws SQLException {
        CurrentStructureUser currentStructureUser = new CurrentStructureUser();
        currentStructureUser.setFirstName(resultSet.getString(1));
        currentStructureUser.setLastName(resultSet.getString(2));
        currentStructureUser.setId(resultSet.getString(3));
        currentStructureUser.setEmail(resultSet.getString(4));
        currentStructureUser.setPhone_1(resultSet.getString(5));
        currentStructureUser.setPhone_2(resultSet.getString(6));
        currentStructureUser.setLogin(resultSet.getString(7));
        currentStructureUser.setCity(resultSet.getString(8));
        currentStructureUser.setPostalCode(resultSet.getString(9));
        currentStructureUser.setAddress(resultSet.getString(10));
        return currentStructureUser;
    }

}
