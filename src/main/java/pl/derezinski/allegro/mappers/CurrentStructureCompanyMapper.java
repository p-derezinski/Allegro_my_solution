package pl.derezinski.allegro.mappers;

import pl.derezinski.allegro.models.CurrentStructureCompany;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CurrentStructureCompanyMapper {

    public CurrentStructureCompany map(ResultSet resultSet) throws SQLException {
        CurrentStructureCompany currentStructureCompany = new CurrentStructureCompany();
        currentStructureCompany.setCompanyName(resultSet.getString(1));
        currentStructureCompany.setNumberNIP(resultSet.getString(2));
        currentStructureCompany.setId(resultSet.getString(3));
        currentStructureCompany.setFirstName(resultSet.getString(4));
        currentStructureCompany.setLastName(resultSet.getString(5));
        currentStructureCompany.setEmail(resultSet.getString(6));
        currentStructureCompany.setPhone_1(resultSet.getString(7));
        currentStructureCompany.setPhone_2(resultSet.getString(8));
        currentStructureCompany.setLogin(resultSet.getString(9));
        currentStructureCompany.setCity(resultSet.getString(10));
        currentStructureCompany.setPostalCode(resultSet.getString(11));
        currentStructureCompany.setAddress(resultSet.getString(12));
        return currentStructureCompany;
    }

}
