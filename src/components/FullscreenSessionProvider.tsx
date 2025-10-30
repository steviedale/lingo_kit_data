import React, { createContext, useContext, ReactNode, useMemo } from "react";
import FullscreenSessionOverlay from "./FullscreenSessionOverlay";
import { SessionDescriptor, useFullscreenSession } from "./useFullscreenSession";

type FullscreenSessionContextValue = {
  isOpen: boolean;
  startSession: (descriptor: SessionDescriptor) => void;
  exitSession: () => void;
  completeSession: () => void;
};

const FullscreenSessionContext = createContext<FullscreenSessionContextValue | null>(null);

type FullscreenSessionProviderProps = {
  children: ReactNode;
};

export const FullscreenSessionProvider: React.FC<FullscreenSessionProviderProps> = ({
  children,
}) => {
  const controller = useFullscreenSession();

  const contextValue = useMemo(
    () => ({
      isOpen: controller.isOpen,
      startSession: controller.open,
      exitSession: controller.exit,
      completeSession: controller.complete,
    }),
    [controller.complete, controller.exit, controller.isOpen, controller.open]
  );

  return (
    <FullscreenSessionContext.Provider value={contextValue}>
      {children}
      <FullscreenSessionOverlay
        isOpen={controller.isOpen}
        onRequestClose={controller.exit}
        title={controller.session?.title}
        data-testid="fullscreen-session-overlay"
      >
        {controller.session?.render({ complete: controller.complete, exit: controller.exit }) ?? null}
      </FullscreenSessionOverlay>
    </FullscreenSessionContext.Provider>
  );
};

export const useFullscreenSessionContext = (): FullscreenSessionContextValue => {
  const context = useContext(FullscreenSessionContext);
  if (!context) {
    throw new Error("useFullscreenSessionContext must be used inside FullscreenSessionProvider");
  }
  return context;
};
