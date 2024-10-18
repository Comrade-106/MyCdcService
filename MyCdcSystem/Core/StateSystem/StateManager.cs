using MyCdcSystem.Models;

namespace MyCdcSystem.Core.StateSystem
{
    public class StateManager
    {
        private readonly StateStore _stateStore;
        private State _currentState;

        public StateManager(StateStore stateStore)
        {
            _stateStore = stateStore;
            _currentState = _stateStore.LoadState();
        }

        public State GetState()
        {
            return _currentState;
        }

        public void UpdateState(Action<State> updateAction)
        {
            updateAction(_currentState);
            _stateStore.SaveState(_currentState);
        }
    }
}
