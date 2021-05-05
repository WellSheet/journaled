module Journaled
  class << self
    def enqueue!(*args)
      delayed_job_enqueue(*args)
    end

    private

    def delayed_job_enqueue(*args, **opts)
      Delayed::Job.enqueue(*args, **opts.reverse_merge(priority: Journaled.job_priority, queue: Journaled.job_queue))
    end
  end
end
