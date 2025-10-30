import { useCallback } from "react";
import { useFullscreenSessionContext } from "../../components/FullscreenSessionProvider";
import { SessionDescriptor } from "../../components/useFullscreenSession";

type LaunchSessionOptions = SessionDescriptor;

type StudySessionLauncher = {
  launchSession: (options: LaunchSessionOptions) => void;
  exitSession: () => void;
  isSessionOpen: boolean;
};

export const useStudySessionLauncher = (): StudySessionLauncher => {
  const { startSession, exitSession, isOpen } = useFullscreenSessionContext();

  const launchSession = useCallback(
    (options: LaunchSessionOptions) => {
      startSession({
        title: options.title,
        render: (controls) =>
          options.render({
            complete: () => {
              controls.complete();
            },
            exit: () => {
              controls.exit();
            },
          }),
      });
    },
    [startSession]
  );

  return {
    launchSession,
    exitSession,
    isSessionOpen: isOpen,
  };
};
