create a SKILL for phase1 execution. Use claude agent teams. each module include 1 senior sde and 1 senior reviwer, reviewer review code and inspect there devloper does not do spec gaming, or play any trick. developer can not
  change integration test, reviwer only have read permission, no permission to write.
  - a dedicate qa team implement integration and run final integration test. qa team only run integration test, create test reports.
  - implemntation include multiple stage
  - stage1, review tasks. each module's senior sde confrim the task and talk to other module's SDE to aling on interface. until both align on tasks and interface.
  - stage2, implemnt tasks, each module's SDE work on their own module, and reviwer review the task.
  - stage3, integration stage, team leader integrate all module's work
  - stage4, test stage, QA run test, create reports. if not pass, ask team leader route issue to module developer. then keep the loop continue.
  - identiy 3-4 human-in-loop inspect point in the entire developer workflow also