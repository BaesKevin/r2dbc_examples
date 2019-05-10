package be.kevinbaes.bap.r2dbcshowcase.domain;

public class Goal {

    private int id;
    private String goal;

    public Goal(int id, String goal) {
        this.id = id;
        this.goal = goal;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getGoal() {
        return goal;
    }

    public void setGoal(String goal) {
        this.goal = goal;
    }

    @Override
    public String toString() {
        return "Goal{" +
                "id=" + id +
                ", goal='" + goal + '\'' +
                '}';
    }
}
