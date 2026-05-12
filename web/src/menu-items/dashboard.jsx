import { Icon } from '@iconify/react';

const icons = {
  IconDashboard: () => <Icon width={20} icon="solar:widget-2-bold-duotone" />,
  IconChartHistogram: () => <Icon width={20} icon="solar:chart-2-bold-duotone" />,
  IconBallFootball: () => <Icon width={20} icon="solar:chat-round-line-bold-duotone" />,
  IconSystemInfo: () => <Icon width={20} icon="solar:code-scan-bold" />,
  IconList: () => <Icon width={20} icon="solar:checklist-minimalistic-bold-duotone" />,
  IconComfyUI: () => <Icon width={20} icon="solar:pallete-2-bold-duotone" />
};

const dashboard = {
  id: 'dashboard',
  title: 'dashboard',
  type: 'group',
  children: [
    {
      id: 'dashboard',
      title: 'dashboard',
      type: 'item',
      url: '/panel/dashboard',
      icon: icons.IconDashboard,
      breadcrumbs: false,
      isAdmin: false
    },
    {
      id: 'analytics',
      title: 'analytics',
      type: 'item',
      url: '/panel/analytics',
      icon: icons.IconChartHistogram,
      breadcrumbs: false,
      isAdmin: true
    },
    {
      id: 'multi_user_stats',
      title: 'multi_user_stats.title',
      type: 'item',
      url: '/panel/multi_user_stats',
      icon: icons.IconList,
      breadcrumbs: false,
      isAdmin: true
    },
    {
      id: 'playground',
      title: 'playground',
      type: 'item',
      url: '/panel/playground',
      icon: icons.IconBallFootball,
      breadcrumbs: false
    },
    {
      id: 'comfyui',
      title: 'ComfyUI',
      type: 'item',
      url: '/panel/comfyui',
      icon: icons.IconComfyUI,
      breadcrumbs: false
    },
    {
      id: 'systemInfo',
      title: 'systemInfo',
      type: 'item',
      url: '/panel/system_info',
      icon: icons.IconSystemInfo,
      breadcrumbs: false,
      isAdmin: true
    }
  ]
};

export default dashboard;
