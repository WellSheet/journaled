module Journaled
  module_function def stream_name_for_app(app_name)
    env_var_name = [app_name&.upcase, 'JOURNALED_STREAM_NAME'].compact.join('_')
    ENV.fetch(env_var_name)
  end
end
