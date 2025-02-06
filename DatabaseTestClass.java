import java.sql.*;
import java.util.*;

public class DatabaseTestClass {
    public static void main(String[] args) {
        Database db = new Database("test_db", "root", "password");
        Table users = new Table("users");
        Column id = new Column("users", "id", null);
        Column name = new Column("users", "name", null);
        Column age = new Column("users", "age", null);

        try {
            // Insert test data
            List<Integer> insertedIds = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                Insert insert = new Insert(users)
                        .column(name, "User " + i)
                        .column(age, 20 + i);
                if (db.execute(insert)) {
                    ResultSet rs = db.request(new Select(users).columns(id).orderBy(new OrderBy(id, OrderByType.DESC)).limit(1));
                    if (rs.next()) {
                        insertedIds.add(rs.getInt(1));
                    }
                }
            }

            // Select queries
            System.out.println("--- All Users ---");
            ResultSet rsAll = db.request(new Select(users).columns(id, name, age));
            while (rsAll.next()) {
                System.out.println("ID: " + rsAll.getInt("id") + " Name: " + rsAll.getString("name") + " Age: " + rsAll.getInt("age"));
            }

            System.out.println("--- Users Older Than 22 ---");
            ResultSet rsFiltered = db.request(new Select(users).columns(id, name, age).where(new Condition(age, ConditionType.GT, 22)));
            while (rsFiltered.next()) {
                System.out.println("ID: " + rsFiltered.getInt("id") + " Name: " + rsFiltered.getString("name") + " Age: " + rsFiltered.getInt("age"));
            }

            // Additional Select Queries
            System.out.println("--- Users with Names Like 'User%' ---");
            ResultSet rsLike = db.request(new Select(users).columns(id, name, age).where(new Condition(name, ConditionType.LIKE, "User%")));
            while (rsLike.next()) {
                System.out.println("ID: " + rsLike.getInt("id") + " Name: " + rsLike.getString("name") + " Age: " + rsLike.getInt("age"));
            }

            System.out.println("--- Oldest User ---");
            ResultSet rsOldest = db.request(new Select(users).columns(id, name, age).orderBy(new OrderBy(age, OrderByType.DESC)).limit(1));
            if (rsOldest.next()) {
                System.out.println("ID: " + rsOldest.getInt("id") + " Name: " + rsOldest.getString("name") + " Age: " + rsOldest.getInt("age"));
            }

            System.out.println("--- Group Users by Age and Count ---");
            ResultSet rsGroupBy = db.request(new Select(users).columns(age).aggregate("COUNT", id, "user_count").groupBy(age));
            while (rsGroupBy.next()) {
                System.out.println("Age: " + rsGroupBy.getInt("age") + " Count: " + rsGroupBy.getInt("user_count"));
            }

            // Update query
            Update update = new Update(users).set(age, 30).where(new Condition(id, ConditionType.EQ, insertedIds.get(0)));
            db.execute(update);

            // Verify update
            System.out.println("--- Updated User ---");
            ResultSet rsUpdated = db.request(new Select(users).columns(id, name, age).where(new Condition(id, ConditionType.EQ, insertedIds.get(0))));
            if (rsUpdated.next()) {
                System.out.println("ID: " + rsUpdated.getInt("id") + " Name: " + rsUpdated.getString("name") + " Age: " + rsUpdated.getInt("age"));
            }

            // Cleanup (delete test data)
            for (int userId : insertedIds) {
                db.execute(new Delete(users).where(new Condition(id, ConditionType.EQ, userId)));
            }

            db.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            db.rollback();
        } finally {
            db.close();
        }
    }
}
