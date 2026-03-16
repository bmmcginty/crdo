class SystemClock < Clock
  def now : Time
    Time.local
  end
end
