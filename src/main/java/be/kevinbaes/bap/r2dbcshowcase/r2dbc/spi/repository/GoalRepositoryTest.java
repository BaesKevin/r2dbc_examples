package be.kevinbaes.bap.r2dbcshowcase.r2dbc.spi.repository;

import be.kevinbaes.bap.r2dbcshowcase.domain.Goal;

public class GoalRepositoryTest {

    public static void main(String[] args) {
        new GoalRepositoryTest().run();
    }

    private void run() {
        goalRepository.deleteAll().block();

        Goal newGoal = new Goal(0, "a goal");

        Goal createdGoal = goalRepository.save(newGoal).block();

        createdGoal.setGoal("updated goal");

        Goal updatedGoal = goalRepository.save(createdGoal).block();

        System.out.println(updatedGoal);
    }

    private final GoalRepository goalRepository;

    public GoalRepositoryTest() {
        this.goalRepository = new GoalRepository();
    }


}
