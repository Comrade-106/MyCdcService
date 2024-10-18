using MyCdcSystem.Models;
using Newtonsoft.Json;

namespace MyCdcSystem.Core.StateSystem
{
    public class StateStore
    {
        private readonly string _stateFilePath = "SystemState";

        public State LoadState()
        {
            if (!File.Exists(_stateFilePath))
            {
                // Возвращаем состояние по умолчанию
                return new State();
            }

            var json = File.ReadAllText(_stateFilePath);
            return JsonConvert.DeserializeObject<State>(json);
        }

        public void SaveState(State state)
        {
            var json = JsonConvert.SerializeObject(state, Formatting.Indented);
            File.WriteAllText(_stateFilePath, json);
        }
    }
}
